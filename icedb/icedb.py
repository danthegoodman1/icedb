import os
from typing import List, Callable
import duckdb
import psycopg2
from typing import List
from uuid import uuid4
import pandas as pd
import duckdb.typing as ty
import psycopg2
import boto3
import botocore
import psycopg2.extensions
import sys

PartitionFunctionType = Callable[[dict], str]
FormatRowType = Callable[[dict], dict]

class IceDB:

    partitionStrategy: PartitionFunctionType
    sortOrder: List[str]
    formatRow: FormatRowType
    ddb: duckdb
    s3region: str
    s3accesskey: str
    s3secretkey: str
    s3endpoint: str
    s3bucket: str
    pgdsn: str
    s3: any
    set_isolation: bool
    unique_row_key: str = None
    custom_merge_query: str = None
    row_group_size: int

    def __init__(
        self,
        partitionStrategy: PartitionFunctionType,
        sortOrder: List[str],
        formatRow: FormatRowType,
        pgdsn: str,
        s3bucket: str,
        s3region: str,
        s3accesskey: str,
        s3secretkey: str,
        s3endpoint: str,
        set_isolation=False,
        create_table=True,
        duckdb_ext_dir: str=None,
        unique_row_key: str=None,
        row_group_size: int=122_880
    ):
        self.partitionStrategy = partitionStrategy
        self.sortOrder = sortOrder
        self.formatRow = formatRow
        self.set_isolation = set_isolation
        self.row_group_size = row_group_size

        self.s3region = s3region
        self.s3accesskey = s3accesskey
        self.s3secretkey = s3secretkey
        self.s3endpoint = s3endpoint
        self.s3bucket = s3bucket

        self.pgdsn = pgdsn

        self.unique_row_key = unique_row_key

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
        self.ddb.execute("SET s3_endpoint='{}'".format(s3endpoint.split("://")[1]))
        self.ddb.execute("SET s3_use_ssl={}".format('false' if "http://" in s3endpoint else 'true'))
        self.ddb.execute("SET s3_url_style='path'")
        if duckdb_ext_dir is not None:
            self.ddb.execute("SET extension_directory='{}'".format(duckdb_ext_dir))

        if create_table:
            conn = self.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        create table if not exists known_files (
                            partition TEXT NOT NULL,
                            filename TEXT NOT NULL,
                            filesize INT8 NOT NULL,
                            active BOOLEAN NOT NULL DEFAULT TRUE,
                            _created TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            _updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            PRIMARY KEY(active, partition, filename)
                        )
                    ''')
                    cursor.execute('commit')
            finally:
                conn.close()
    def close(self):
        pass

    def getconn(self):
        pgc = psycopg2.connect(self.pgdsn)
        pgc.autocommit = False
        return pgc

    def insert(self, rows: List[dict]) -> List[str]:
        """
        Creates one or more files in the destination folder based on the partition strategy
        :param rows: Rows of JSON data to be inserted. Must have the expected keys of the partitioning strategy and the sorting order
        """
        partmap = {}
        for row in rows:
            # merge the rows into same parts
            part = self.partitionStrategy(row)
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
            first_row = self.formatRow(partrows[0].copy()) # need to make copy so we don't modify
            first_row['_row_id'] = str(uuid4()) if self.unique_row_key is None else partrows[0][self.unique_row_key]
            for key in first_row:
                # convert everything to array
                first_row[key] = [first_row[key]]
            df = pd.DataFrame(first_row)
            if len(partrows) > 1:
                # we need to add more rows
                for row in partrows[1:]:
                    new_row = self.formatRow(row.copy()) # need to make copy so we don't modify
                    new_row['_row_id'] = str(uuid4()) if self.unique_row_key is None else row[self.unique_row_key]
                    df.loc[len(df)] = new_row

            # copy to parquet file
            self.ddb.sql('''
                copy (select * from df order by {}) to '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})
            '''.format(', '.join(self.sortOrder), 's3://{}/{}'.format(self.s3bucket, fullpath), self.row_group_size))

            # get file metadata
            obj = self.s3.head_object(
                Bucket=self.s3bucket,
                Key=fullpath
            )
            fileSize = obj['ContentLength']

            # insert into meta store
            conn = self.getconn()
            try:
                with conn.cursor() as cursor:
                    cursor.execute('''
                        insert into known_files (filename, filesize, partition)  VALUES ('{}', {}, '{}')
                    '''.format(filename, fileSize, part))
                    cursor.execute('commit')
            finally:
                conn.close()

        return final_files

    def merge_files(self, maxFileSize, maxFileCount=10, asc=False, partition_prefix: str=None, custom_merge_query: str=None) -> int:
        '''
        desc merge should be fast, working on active partitions. asc merge should be slow and in background,
        slowly fully optimizes partitions over time.

        Returns the number of files merged.
        '''
        # cursor scan active files in the direction
        curid = "a"+str(uuid4()).replace("-", "")
        buf = []
        fsum = 0
        conn = self.getconn()
        try:
            with conn.cursor() as mycur:
                if self.set_isolation:
                    # don't need serializable isolation here, just need a snapshot
                    # if the files change between the next transaction, then they will be omitted from the first query selecting them
                    mycur.execute("set transaction isolation level repeatable read")

                # need to manually start cursor because this is "not in a transaction yet"?
                mycur.execute('''
                declare {} cursor for
                select partition, filename, filesize
                from known_files
                where active = true
                and filesize < {}
                {}
                order by partition {}
                '''.format(
                    curid,
                    maxFileSize,
                    '' if partition_prefix is None else "and partition LIKE '{}%'".format(partition_prefix),
                    'asc' if asc else 'desc')
                )
                keepGoing = True
                while keepGoing:
                    mycur.execute("""
                    fetch forward {} from {}
                    """.format(200, curid))
                    rows = mycur.fetchall()
                    if len(rows) == 0:
                        conn.rollback()
                        keepGoing = False
                        break
                    for row in rows:
                        if len(buf) > 0 and row[0] != buf[0][0]:
                            if len(buf) > 1:
                                # we've hit the end of the partition and we can merge it
                                print("I've hit the end of the partition with files to merge")
                                conn.rollback()
                                keepGoing = False
                                break

                            # we've hit the next partition, clear the buffer
                            print('buffer exceeded for {}, going to next partition'.format(buf[0][0]))
                            buf = []
                            fsum = 0

                        # check if we would exceed the max file size
                        if len(buf) > 1 and fsum > maxFileSize:
                            print('I hit the max file size with {} bytes, going to start merging!'.format(fsum))
                            conn.rollback()
                            keepGoing = False
                            break

                        # check if we exceeded the max file count, only if valid count
                        if len(buf) > 1 and len(buf) >= maxFileCount:
                            print('I hit the max file count with {} files, going to start merging!'.format(len(buf)))
                            conn.rollback()
                            keepGoing = False
                            break

                        buf.append(row)
                        fsum += row[2]
        except Exception as e:
            print("failed merge", e)
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            conn.rollback()
        finally:
            conn.close()

        # select the files for update to make sure they are all still active, anything not active we drop (from colliding merges)
        # if there is only 1 file, then we have nothing to merge
        if len(buf) > 1:
            partition = buf[0][0]
            # merge these files, update DB
            print('I have files for merging! going to lock them now, len:', len(buf))
            conn = self.getconn()
            try:
                with conn.cursor() as mergecur:
                    # lock the files up
                    if self.set_isolation:
                        # now we need serializable isolation to protect against concurrent merges
                        mergecur.execute("set transaction isolation level serializable")

                    q = '''
                        select filename
                        from known_files
                        where active = true
                        and partition = '{}'
                        and filename in ({})
                        for update
                    '''.format(partition, ','.join(list(map(lambda x: "'{}'".format(x[1]), buf))))
                    mergecur.execute(q)
                    rows = mergecur.fetchall()
                    actual_files = list(map(lambda x: x[0], rows))

                    if len(actual_files) == 0:
                        print('no actual files during merge, were there competing merges? I am exiting.')
                        return 0
                    if len(actual_files) == 1:
                        print('only got a single file when locking files for merging, were there competing merges? I am exiting.')
                        return 0
                    new_f_name = '{}.parquet'.format(str(uuid4()))
                    new_f_path = partition + "/" + new_f_name
                    
                    # copy the files in S3
                    q = '''
                    COPY (
                        {}
                    ) TO '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})
                    '''.format(
                        ('''
                        select *
                        from source_files
                        ''' if custom_merge_query is None else custom_merge_query).replace("source_files", "read_parquet(?, hive_partitioning=1)"),
                        's3://{}/{}'.format(self.s3bucket, new_f_path),
                        self.row_group_size
                    )

                    self.ddb.execute(q, [
                        list(map(lambda x: "s3://{}/{}/{}".format(self.s3bucket, partition, x), actual_files))
                    ])

                    # get the new file size
                    obj = self.s3.head_object(
                        Bucket=self.s3bucket,
                        Key=new_f_path
                    )
                    new_f_size = obj['ContentLength']

                    # update the old files
                    q = '''
                        update known_files
                        set active = false
                        , _updated = NOW()
                        where active = true
                        and partition = '{}'
                        and filename in ({})
                    '''.format(partition, ','.join(list(map(lambda x: "'{}'".format(x), actual_files))))
                    mergecur.execute(q)

                    # insert the new file
                    mergecur.execute('''
                        insert into known_files (filename, filesize, partition)  VALUES ('{}', {}, '{}')
                    '''.format(new_f_name, new_f_size, partition))

                    mergecur.execute('commit')
                    print('merged files', actual_files)
                    return len(actual_files)
            except:
                conn.rollback()
                raise
            finally:
                conn.close()
        return 0

    def get_files(self, gte_part: str, lte_part: str) -> List[str]:
        conn = self.getconn()
        try:
            with conn.cursor() as mycur:
                mycur.execute('''
                select partition, filename
                from known_files
                where active = true
                AND partition >= %s
                AND partition <= %s
                ''', (gte_part, lte_part))
                rows = mycur.fetchall()
                return list(map(lambda x: 's3://{}/{}/{}'.format(self.s3bucket, x[0], x[1]), rows))
        finally:
            conn.close()
