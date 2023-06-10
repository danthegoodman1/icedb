import os
from typing import List, Callable
import duckdb
import psycopg2
from typing import List
from datetime import datetime
from uuid import uuid4
import pandas as pd
import duckdb.typing as ty
import psycopg2
import boto3
import botocore

PartitionFunctionType = Callable[[dict], List[List[str]]]

class IceDB:

    partitionStrategy: PartitionFunctionType
    sortOrder: List[str]
    ddb: duckdb
    conn: psycopg2._T_conn
    s3region: str
    s3accesskey: str
    s3secretkey: str
    s3endpoint: str
    s3bucket: str
    pgdsn: str
    s3: any

    def __init__(
        self,
        partitionStrategy: PartitionFunctionType,
        sortOrder: List[str],
        pgdsn=os.environ['PG_DSN'],
        s3bucket=os.environ['S3_BUCKET'],
        s3region=os.environ['S3_REGION'],
        s3accesskey=os.environ['AWS_ACCESS_KEY_ID'],
        s3secretkey=os.environ['AWS_SECRET_ACCESS_KEY'],
        s3endpoint=os.environ['S3_ENDPOINT'],
    ):
        self.partitionStrategy = partitionStrategy
        self.sortOrder = sortOrder
        
        self.s3region = s3region
        self.s3accesskey = s3accesskey
        self.s3secretkey = s3secretkey
        self.s3endpoint = s3endpoint
        self.s3bucket = s3bucket
        
        self.pgdsn = pgdsn
        self.conn = psycopg2.connect(pgdsn)
        self.conn.autocommit = True

        self.session = boto3.session.Session()
        self.s3 = self.session.client('s3',
            config=botocore.config.Config(s3={'addressing_style': 'path'}),
            region_name=s3region,
            endpoint_url=s3endpoint,
            aws_access_key_id=s3accesskey,
            aws_secret_access_key=s3secretkey
        )

        self.ddb = duckdb.connect(":memory:")
        self.ddb.execute("install httpfs")
        self.ddb.execute("load httpfs")
        self.ddb.execute("SET s3_region='{}'".format(s3region))
        self.ddb.execute("SET s3_access_key_id='{}'".format(s3accesskey))
        self.ddb.execute("SET s3_secret_access_key='{}'".format(s3secretkey))
        self.ddb.execute("SET s3_endpoint='{}'".format(s3endpoint))
        self.ddb.execute("SET s3_use_ssl={}".format('true' if 'https' in s3endpoint else 'false'))
        self.ddb.execute("SET s3_url_style='path'")

        # trick for using autocommit
        with self.conn:
            with self.conn.cursor() as cursor:
                # make sure the table exists
                cursor.execute('''
                    create table if not exists known_files (
                        partition TEXT NOT NULL,
                        filename TEXT NOT NULL,
                        filesize INT8 NOT NULL,
                        active BOOLEAN NOT NULL DEFAULT TRUE,
                        PRIMARY KEY(active, partition, filename)
                    )
                ''')

    def insert(self, rows: List[dict]) -> List[str]:
        """
        Creates one or more files in the destination folder based on the partition strategy
        :param rows: Rows of JSON data to be inserted. Must have the expected keys of the partitioning strategy and the sorting order
        """
        partmap = {}
        for row in rows:
            # merge the rows into same parts
            part = self.partStrategy(row)
            if part not in partmap:
                partmap[part] = []
            partmap[part].append(row)

        final_files = []
        for part in partmap:
            # upload parquet file
            filename = '{}.parquet'.format(uuid4())
            fullpath = part + '/' + filename
            final_files.append(fullpath)
            partrows = partmap[part]
            
            # use a DF for inserting into duckdb
            df = pd.DataFrame(partrows[0])
            if len(partrows) > 1:
                # we need to add more rows
                for row in partrows[1:]:
                    df.loc[len(df)] = row

            # copy to parquet file
            self.ddb.sql('''
                copy (select * from df order by {}) to '{}'
            '''.format(', '.join(self.sortOrder), 's3://{}/{}'.format(self.s3bucket, fullpath)))

            # get file metadata
            obj = self.s3.head_object(
                Bucket=self.s3bucket,
                Key=fullpath
            )
            fileSize = obj['ContentLength']

            # insert into meta store
            with self.conn:
                with self.conn.cursor() as cursor:
                    cursor.execute('''
                        insert into known_files (filename, filesize, partition)  VALUES ('{}', {}, '{}')
                    '''.format(filename, fileSize, part))

        return final_files

    def sumfilesizes(self, arr) -> int:
        t = 0
        for i in arr:
            t += i[2]
        return t
    
    def merge_files(self, maxFileSize, maxFileCount=10, asc=False):
        '''
        desc merge should be fast, working on active partitions. asc merge should be slow and in background,
        slowly fully optimizes partitions over time.
        '''
        # cursor scan active files in the direction
        curid = str(uuid4())
        buf = []
        with self.conn:
            with self.conn.cursor(curid) as mycur:
                mycur.itersize = 200 # get 200 rows at a time
                mycur.execute('''
                select partition, filename, filesize
                from known_files
                where active = true
                and filesize < {}
                order by partition {}
                '''.format(maxFileSize, 'asc' if asc else 'desc'))
                for row in mycur:
                    if len(buf) > 0 and row[0] != buf[0][0]:
                        if len(buf) > 1:
                            # we've hit the end of the partition and we can merge it
                            print("I've hit the end of the partition with files to merge")
                            break

                        # we've hit the next partition, clear the buffer
                        print('buffer exceeded for {}, going to next partition'.format(buf[0][0]))
                        buf = []

                    # check if we would exceed the max file size
                    fsum = self.sumfilesizes(buf)
                    if len(buf) > 1 and fsum > maxFileSize:
                        print('I hit the max file size with {} bytes, going to start merging!'.format(fsum))
                        break

                    # check if we exceeded the max file count, only if valid count
                    if len(buf) > 1 and len(buf)-1 >= maxFileCount:
                        print('I hit the max file count with {} files, going to start merging!'.format(len(buf)))
                        break

                    buf.append(row)

            # select the files for update to make sure they are all still active, anything not active we drop (from colliding merges)
            if len(buf) > 0:
                # merge these files, update DB
                print('I have files for merging! going to lock them now')
                with self.conn.cursor() as mergecur:
                    # lock the files up
                    mergecur.execute('''
                        select filename
                        from known_files
                        where active = true
                        and partition = '{}'
                        and filename in ({})
                        for update
                    '''.format(buf[0][0], ','.join(list(map(lambda x: "'{}'".format(x[2]), buf)))))
                    new_f_name = '{}.parquet'.format(str(uuid4()))
                    new_f_path = buf[0][0] + "/" + new_f_name
                    # copy the files in S3
                    q = '''
                    COPY (
                        select *
                        from read_parquet([{}], hive_partitioning=1)
                    ) TO 's3://{}/{}'
                    '''.format(','.join(list(map(lambda x: "'s3://{}/{}/{}'".format(self.s3bucket, x[0], x[1]), buf))), self.s3bucket, new_f_path)
                    self.ddb.execute(q)

                    # get the new file size
                    obj = self.s3.head_object(
                        Bucket=self.s3bucket,
                        Key=new_f_path
                    )
                    new_f_size = obj['ContentLength']

                    # insert the new file
                    mergecur.execute('''
                        insert into known_files (filename, filesize, partition)  VALUES ('{}', {}, '{}')
                    '''.format(new_f_name, new_f_size, buf[0][0]))

                    # update the old files
                    q = '''
                        update known_files
                        set active = false
                        where active = true
                        and partition = '{}'
                        and filename in ({})
                    '''.format(buf[0][0], ','.join(list(map(lambda x: "'{}'".format(x[1]), buf))))
                    mergecur.execute(q)
