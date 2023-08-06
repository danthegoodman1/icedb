from icedb import IceDBv3
from log import S3Client, IceLogIO, SchemaConflictException
from datetime import datetime
import json
from time import time

s3c = S3Client(s3prefix="tenant", s3bucket="testbucket", s3region="us-east-1", s3endpoint="http://localhost:9000",
               s3accesskey="user", s3secretkey="password")


def part_func(row: dict) -> str:
    row_time = datetime.utcfromtimestamp(row['ts'] / 1000)
    part = f"cust=test/d={row_time.strftime('%Y-%m-%d')}"
    return part


def format_row(row: dict) -> dict:
    row['properties'] = json.dumps(row['properties'])  # convert nested dict to json string
    return row


ice = IceDBv3(
    part_func,
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

example_events = [
    {
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
    }
]
more_example_events = [
    {
        "ts": 1686176939445,
        "event": "page_load",
        "user_id": "a",
        "properties": {
            "hey": "hoeeeeeee",
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
            "hey": "hoergeeeeeeeeeeeeeeergergrergereg",
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
            "hey": "hoeeeeeeeeeeeeeeeee",
            "numtime": 1,
            "nested_dict": {
                "ee": "fff"
            }
        }
    }
]

try:
    print("============= inserting ==================")
    inserted = ice.insert(example_events)
    firstInserted = list(map(lambda x: x.path, inserted))
    print('inserted', firstInserted)

    # Read the state in
    log = IceLogIO("dan-mbp")
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("============= Current State =============")
    print("schema:", s1)
    print("files:", f1)
    print("tombstones:", t1)
    print("log files:", l1)

    # Verify the results
    query = "select count(user_id), user_id from read_parquet([{}]) group by user_id order by count(user_id) desc".format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", f1)))
    )
    print('executing query:', query)
    # THIS IS A BAD IDEA NEVER DO THIS IN PRODUCTION
    ice.ddb.execute(query)
    res = ice.ddb.fetchall()
    print(res)

    assert res[0][0] == 2
    assert res[1][0] == 1

    print('results validated')

    # ================= do it again =====================
    print("============= inserting ==================")
    inserted = ice.insert(more_example_events)
    secondInserted = list(map(lambda x: x.path, inserted))
    print('inserted', secondInserted)

    # Read the state in
    log = IceLogIO("dan-mbp")
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("=== Current State ===")
    print("schema:", s1)
    print("files:", f1)
    print("tombstones:", t1)
    print("log files:", l1)

    # Verify the results
    query = "select count(user_id), user_id from read_parquet([{}]) group by user_id order by count(user_id) desc".format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", f1)))
    )
    print('executing query:', query)
    # THIS IS A BAD IDEA NEVER DO THIS IN PRODUCTION
    ice.ddb.execute(query)
    res = ice.ddb.fetchall()
    print(res)

    assert res[0][0] == 4
    assert res[1][0] == 2

    # ================= one more time =====================
    print("============= inserting ==================")
    inserted = ice.insert(more_example_events)
    third_inserted = list(map(lambda x: x.path, inserted))
    print('inserted', third_inserted)

    # Read the state in
    log = IceLogIO("dan-mbp")
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("=== Current State ===")
    print("schema:", s1)
    print("files:", f1)
    print("tombstones:", t1)
    print("log files:", l1)

    # Verify the results
    query = "select count(user_id), user_id from read_parquet([{}]) group by user_id order by count(user_id) desc".format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", f1)))
    )
    print('executing query:', query)
    # THIS IS A BAD IDEA NEVER DO THIS IN PRODUCTION
    ice.ddb.execute(query)
    res = ice.ddb.fetchall()
    print(res)

    assert res[0][0] == 6
    assert res[1][0] == 3

    print('results validated')

    # ================== Merge =========================
    print("============== merging results ==============")

    previous_logs = sorted(l1)

    # merge fully
    l1, new_file, partition, merged_files, meta = ice.merge(max_file_count=2)
    if l1 is not None:
        print("merged", l1, new_file, partition, merged_files)

    # Read the state in
    log = IceLogIO("dan-mbp")
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("=== Current State ===")
    print("schema:", s1)
    print("files:", f1)
    print("tombstones:", t1)
    print("log files:", l1)

    # verify log tombstones and file marker tombstones
    expectedFileTombstones = list(filter(lambda x: 'd=2023-06-07' in x, firstInserted+secondInserted))
    print("expecting file tombstones:", expectedFileTombstones)
    actual_tomb = list(map(lambda x: x.path, filter(lambda x: x.tombstone is not None, f1)))
    print("got actual tombstones:", actual_tomb)
    assert sorted(expectedFileTombstones) == sorted(actual_tomb)

    expected_log_tombstones = previous_logs[:2]
    print("expected log tombstones", expected_log_tombstones)
    actual_log_tombstones = sorted(map(lambda x: x.path, t1))
    print("actual log tombstones", actual_log_tombstones)
    assert expected_log_tombstones == actual_log_tombstones

    # Verify the results
    alive_files = list(filter(lambda x: x.tombstone is None, f1))
    query = "select count(user_id), user_id from read_parquet([{}]) group by user_id order by count(user_id) desc".format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", alive_files)))
    )
    print('executing query:', query)
    # THIS IS A BAD IDEA NEVER DO THIS IN PRODUCTION
    ice.ddb.execute(query)
    res = ice.ddb.fetchall()
    print(res)

    assert res[0][0] == 6
    assert res[1][0] == 3

    print('results validated')


    print("============= insert conflicting types ==================")
    conflicting_out = ice.insert([{
        "ts": 1686176939445,
        "event": 1,
        "user_id": 1.2,
        "properties": {
            "hey": "ho",
            "numtime": 1,
            "nested_dict": {
                "ee": "fff"
            }
        }
    }])
    try:
        # Read the state in
        log = IceLogIO("dan-mbp")
        s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
        print("=== Current State ===")
        print("schema:", s1)
        print("files:", f1)
        print("tombstones:", t1)
        print("log files:", l1)
    except SchemaConflictException as e:
        print("Caught schema conflict, this should normally be handled at insert time")

    print("test successful!")
except Exception as e:
    # print('exception:', type(e).__name__, e)
    raise e
finally:
    # ================== Clean up =========================
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
