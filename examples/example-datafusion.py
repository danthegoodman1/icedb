# NOT WORKING YET
from icedb import IceDB
from datetime import datetime
import json
from datafusion import SessionContext, udf
import pyarrow as pa

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
    s3accesskey="Ia3NaZPuGcOEoHIJr6mZ",
    s3secretkey="pS5gWnpb7yErrQzhlhzE62ir4UNbUQ6PGqOvth5d",
    s3endpoint="http://localhost:9000",
    create_table=True,
    formatRow=format_row
)

def get_partition_range(syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    return ['y={}/m={}/d={}'.format('{}'.format(syear).zfill(4), '{}'.format(smonth).zfill(2), '{}'.format(sday).zfill(2)),
            'y={}/m={}/d={}'.format('{}'.format(eyear).zfill(4), '{}'.format(emonth).zfill(2), '{}'.format(eday).zfill(2))]

def get_files(syear: pa.int64, smonth: pa.int64, sday: pa.int64, eyear: pa.int64, emonth: pa.int64, eday: pa.int64) -> pa.string:
    part_range = get_partition_range(syear, smonth, sday, eyear, emonth, eday)
    return pa.string('{}{}'.format(part_range[0],
        part_range[1]))
    return ice.get_files(
        part_range[0],
        part_range[1]
    )

# Create a DataFusion context
ctx = SessionContext()

get_files_udf = udf(
    get_files,
    [pa.int64(), pa.int64(), pa.int64(), pa.int64(), pa.int64(), pa.int64()],
    pa.string(),
    "stable",
    name="get_files",
)

# Register UDF for use in SQL
ctx.register_udf(get_files_udf)

result_df = ctx.sql("select get_files(2023,2,1, 2023,8,1)")
print(result_df)
