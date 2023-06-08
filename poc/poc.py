import os
from typing import List, Callable
import time
from datetime import datetime
from uuid import uuid4
import duckdb
import pandas as pd
import json
import pyarrow as pa
import duckdb.typing as ty

example_events = [{
    "ts": 1686176939445,
    "event": "page_load",
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
    "properties": {
        "hey": "ho",
        "numtime": 222,
        "nested_dict": {
            "ee": "fff"
        }
    }
}]

ddb = duckdb.connect("ddb")
ddb.execute('''
    create table if not exists known_files (
        year TEXT NOT NULL,
        month TEXT NOT NULL,
        day TEXT NOT NULL,
        filename TEXT NOT NULL,
        active BOOLEAN NOT NULL DEFAULT TRUE,
        PRIMARY KEY(year, month, day, filename)
    )
''')

def insertRows(rows: List[dict]):
    partmap = {}
    for row in rows:
        # merge the rows into same parts
        rowTime = datetime.utcfromtimestamp(row['ts']/1000)
        part = 'y={}/m={}/d={}/'.format('{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2))
        if part not in partmap:
            partmap[part] = []
        partmap[part].append(row)

    final_files = []
    for part in partmap:
        # upload parquet file
        filename = '{}.parquet'.format(uuid4())
        fullpath = part + filename
        final_files.append(fullpath)
        partrows = partmap[part]
        # let's just make it a df to make things easy
        df = pa.Table.from_pydict({
            'ts': map(lambda row: row['ts'], partrows),
            'event': map(lambda row: row['event'], partrows),
            'properties': map(lambda row: json.dumps(row['properties']), partrows), # turn the properties into string
            'row_id': map(lambda row: str(uuid4()), partrows) # give every row a unique id for dedupe
        })
        ddb.sql('''
            copy (select * from df order by event, ts) to '{}'
        '''.format('files/' + fullpath)) # order by event, then ts as we are probably grabbing a single event for each query

        # insert into meta store
        rowTime = datetime.utcfromtimestamp(partrows[0]['ts'] / 1000) # this is janky
        ddb.execute('''
            insert into known_files (year, month, day, filename)  VALUES (?, ?, ?, ?)
        ''', ['{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2), filename])

    return final_files

def get_files(year: int, month: int, day: int) -> list[str]:
  raw_files = os.listdir('files/y={}/m={}/d={}/'.format('{}'.format(year).zfill(4), '{}'.format(month).zfill(2), '{}'.format(day).zfill(2)))
  return list(map(lambda x: 'files/y={}/m={}/d={}/{}'.format('{}'.format(year).zfill(4), '{}'.format(month).zfill(2), '{}'.format(day).zfill(2), x), raw_files))

ddb.create_function('get_files', get_files, [ty.INTEGER, ty.INTEGER, ty.INTEGER], list[str])


final_files = insertRows(example_events)
print('inserted files', final_files)
print(ddb.sql('''
    select * from read_parquet('files/**', hive_partitioning=1, filename=1)
'''))
# merging is omitted for this demo, as it's a pretty simple

# normally this would be a time range, but for poc it's easier to not have to calculate that
print(ddb.sql('''
    select sum((properties::JSON->>'numtime')::int64)
    from read_parquet(get_files(2023, 6, 7), hive_partitioning=1, filename=1)
    where event = 'page_load'
'''))