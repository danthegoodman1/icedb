import os
from typing import List
from datetime import datetime
from uuid import uuid4
import duckdb
import json
import pyarrow as pa
import duckdb.typing as ty
import psycopg2
import boto3
import botocore

conn = psycopg2.connect(
    host="localhost",
    port=26257,
    user="root",
    database="defaultdb"
)

example_events = [{
    "ts": 1686176939445,
    "event": "page_load",
    "user_id": "a",
    "properties": {
        "hey": "ho",
        "numtime": 123,
        "nested_dict": {
            "ee": "fff"
        }
    }
}, {
    "ts": 1676126229999,
    "event": "page_load",
    "user_id": "b",
    "properties": {
        "hey": "ho",
        "numtime": 933,
        "nested_dict": {
            "ee": "fff"
        }
    }
}, {
    "ts": 1686176939666,
    "event": "something_else",
    "user_id": "a",
    "properties": {
        "hey": "ho",
        "numtime": 222,
        "nested_dict": {
            "ee": "fff"
        }
    }
}]

ddb = duckdb.connect(":memory:")
cursor = conn.cursor()
cursor.execute('''
    create table if not exists known_files (
        partition TEXT NOT NULL,
        filename TEXT NOT NULL,
        filesize INT8 NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        PRIMARY KEY(active, partition, filename)
    )
''')
conn.commit()

ddb.execute('''
install httpfs
''')
ddb.execute('''
load httpfs
''')
ddb.execute('''
SET s3_region='us-east-1'
''')
ddb.execute('''
SET s3_access_key_id='x20QiZlFwUh66jUj3GuT'
''')
ddb.execute('''
SET s3_secret_access_key='OJGdsO363fFQo0DFYeula4JJdCLAmVWZWJnPy4IG'
''')
ddb.execute('''
SET s3_endpoint='localhost:9000'
''')
ddb.execute('''
SET s3_use_ssl=false
''')
ddb.execute('''
SET s3_url_style='path'
''')

session = boto3.session.Session()
s3 = session.client('s3',
    config=botocore.config.Config(s3={'addressing_style': 'path'}),
    region_name='us-east-1',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='x20QiZlFwUh66jUj3GuT',
    aws_secret_access_key='OJGdsO363fFQo0DFYeula4JJdCLAmVWZWJnPy4IG'
)

def insertRows(rows: List[dict]):
    partmap = {}
    for row in rows:
        # merge the rows into same parts
        rowTime = datetime.utcfromtimestamp(row['ts']/1000)
        part = 'y={}/m={}/d={}'.format('{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2))
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
        df = pa.Table.from_pydict({
            'ts': map(lambda row: row['ts'], partrows),
            'event': map(lambda row: row['event'], partrows),
            'properties': map(lambda row: json.dumps(row['properties']), partrows), # turn the properties into string
            'row_id': map(lambda row: str(uuid4()), partrows) # give every row a unique id for dedupe
        })
        ddb.sql('''
            copy (select * from df order by event, ts) to '{}'
        '''.format('s3://testbucket/' + fullpath)) # order by event, then ts as we are probably grabbing a single event for each query

        # get file metadata
        obj = s3.head_object(
            Bucket='testbucket',
            Key=fullpath
        )
        fileSize = obj['ContentLength']

        # insert into meta store
        rowTime = datetime.utcfromtimestamp(partrows[0]['ts'] / 1000) # this is janky
        cursor.execute('''
            insert into known_files (filename, filesize, partition)  VALUES ('{}', {}, '{}')
        '''.format(filename, fileSize, part))
        conn.commit()

    return final_files

def get_files(syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    # can't use duckdb here otherwise the db will lock up due to nested queries
    q = '''
    select partition, filename
    from known_files
    where active = true
    AND partition >= 'y={}/m={}/d={}'
    AND partition <= 'y={}/m={}/d={}'
    '''.format('{}'.format(syear).zfill(4), '{}'.format(smonth).zfill(2), '{}'.format(sday).zfill(2),'{}'.format(eyear).zfill(4), '{}'.format(emonth).zfill(2), '{}'.format(eday).zfill(2))
    cursor.execute(q)
    rows = cursor.fetchall()
    return list(map(lambda x: 's3://testbucket/{}/{}'.format(x[0], x[1]), rows))

ddb.create_function('get_files', get_files, [ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER], list[str])

ddb.sql('''
    create macro if not exists get_f(start_year:=2023, start_month:=1, start_day:=1, end_year:=2023, end_month:=1, end_day:=1) as get_files(start_year, start_month, start_day, end_year, end_month, end_day)
''')

def merge_files(maxFileSize, asc=False):
    '''
    desc merge should be fast, working on active partitions. asc merge should be slow and in background,
    slowly fully optimizes partitions over time.
    '''
    # cursor scan active files in the direction

    # select the files for update to make sure they are all still active, anything not active we drop (from colliding merges)

    # merge these files, update DB
    pass

final_files = insertRows(example_events)
print('inserted files', final_files)

# show what it looks like
print(ddb.sql('''
select count(*) as num_active_files
from UNNEST(get_f(end_year:=2024))
'''))

# merge files
merge_files(100_000)

print(ddb.sql('''
select count(*) as num_active_files_after_merge
from UNNEST(get_f(end_year:=2024))
'''))

# merge files

# query files
print(ddb.sql('''
    select sum((properties::JSON->>'numtime')::int64)
    from read_parquet(get_f(start_month:=2, end_month:=8), hive_partitioning=1)
    where event = 'page_load'
'''))

cursor.close()
