from icedb import IceDBv3
from log import S3Client, IceLogIO
from datetime import datetime
import json
from time import time

s3c = S3Client(s3prefix="tenant", s3bucket="testbucket", s3region="us-east-1", s3endpoint="http://localhost:9000", s3accesskey="user", s3secretkey="password")

def partStrat(row: dict) -> str:
    rowTime = datetime.utcfromtimestamp(row['ts']/1000)
    part = f"d={rowTime.strftime('%Y-%m-%d')}"
    return part

def format_row(row: dict) -> dict:
    row['properties'] = json.dumps(row['properties']) # convert nested dict to json string
    return row

ice = IceDBv3(
    partStrat,
['event', 'ts'],
    format_row,
    "us-east-1",
    "user",
    "password",
    "http://localhost:9000",
    s3c,
    "dan-mbp",
    True
)

example_events = [{
    "ts": 1686176939445,
    "event": "page_load",
    "user_id": "a",
    "properties": {
        "hey": "ho",
        "numtime": 1,
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
        "numtime": 1,
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
        "numtime": 1,
        "nested_dict": {
            "ee": "fff"
        }
    }
}]


inserted = ice.insert(example_events)
print('inserted', list(map(lambda x: x.path, inserted)))

# Read the state in
log = IceLogIO("dan-mbp")
s1, f1, t1 = log.read_at_max_time(s3c, round(time() * 1000))
print("============= Current State =============")
print(s1.toJSON())
print(list(map(lambda x: x.path, f1)))
print(list(map(lambda x: x.path, t1)))


# Clean up
clean = True
if clean:
    logFiles = s3c.s3.list_objects_v2(
        Bucket=s3c.s3bucket,
        MaxKeys=1000,
        Prefix=s3c.s3prefix
    )
    for file in logFiles['Contents']:
        key = file['Key']
        print('deleting', key)
        s3c.s3.delete_object(
            Bucket=s3c.s3bucket,
            Key=key
        )