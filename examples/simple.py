from icedb import IceDB
from datetime import datetime
import duckdb
import duckdb.typing as ty
import json

def partStrat(row: dict) -> str:
    rowTime = datetime.utcfromtimestamp(row['ts']/1000)
    part = 'y={}/m={}/d={}'.format('{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2))
    return part

def format_row(row: dict) -> dict:
    row['properties'] = json.dumps(row['properties']) # convert nested dict to json string
    return row

ice = IceDB(
    partitionStrategy=partStrat,
    sortOrder=['event', 'ts'],
    pgdsn="postgresql://root@localhost:26257/defaultdb",
    s3bucket="testbucket",
    s3region="us-east-1",
    s3accesskey="user",
    s3secretkey="password",
    s3endpoint="http://localhost:9000",
    create_table=True,
    formatRow=format_row
)

def get_partition_range(syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    return ['y={}/m={}/d={}'.format('{}'.format(syear).zfill(4), '{}'.format(smonth).zfill(2), '{}'.format(sday).zfill(2)),
            'y={}/m={}/d={}'.format('{}'.format(eyear).zfill(4), '{}'.format(emonth).zfill(2), '{}'.format(eday).zfill(2))]

def get_files(syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    part_range = get_partition_range(syear, smonth, sday, eyear, emonth, eday)
    return ice.get_files(
        part_range[0],
        part_range[1]
    )



# This section is just for preparing to query stuff
ddb = duckdb.connect(":memory:")
ddb.execute("install httpfs")
ddb.execute("load httpfs")
ddb.execute("SET s3_region='us-east-1'")
ddb.execute("SET s3_access_key_id='user'")
ddb.execute("SET s3_secret_access_key='password'")
ddb.execute("SET s3_endpoint='localhost:9000'")
ddb.execute("SET s3_use_ssl=false")
ddb.execute("SET s3_url_style='path'")
ddb.create_function('get_files', get_files, [ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER], list[str])
ddb.sql('''
    create macro if not exists get_f(start_year:=2023, start_month:=1, start_day:=1, end_year:=2023, end_month:=1, end_day:=1) as get_files(start_year, start_month, start_day, end_year, end_month, end_day)
''')
ddb.sql('''
    create macro if not exists icedb(start_year:=2023, start_month:=1, start_day:=1, end_year:=2023, end_month:=1, end_day:=1) as table select * from read_parquet(get_files(start_year, start_month, start_day, end_year, end_month, end_day), hive_partitioning=1, filename=1)
''')

# some sample data
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
        "hey": "hoergergergrergereg",
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


inserted = ice.insert(example_events)
print('inserted', inserted)

# see the number of files and aggregation result before we merge
print('got files', len(get_files(2023,1,1, 2023,8,1)))
print(ddb.sql('''
select sum((properties::JSON->>'numtime')::int64) as agg, extract('month' from epoch_ms(ts)) as month
from icedb(start_month:=2, end_month:=8)
where event = 'page_load'
group by month
order by agg desc
'''))

merged = ice.merge_files(100_000)
print('merged', merged)
merged = ice.merge_files(100_000)
print('merged again', merged)

# you will see the aggregation values are the same after the merge
print(ddb.sql('''
select sum((properties::JSON->>'numtime')::int64) as agg, extract('month' from epoch_ms(ts)) as month
from icedb(start_month:=2, end_month:=8)
where event = 'page_load'
group by month
order by agg desc
'''))

print(ddb.sql('''
select count(*), filename
from icedb(start_month:=2, end_month:=8)
group by filename
'''))

# you'll see the file count is smaller now
print('got files', len(get_files(2020,1,1, 2024,8,1)))
