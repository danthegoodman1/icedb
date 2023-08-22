from icedb.icedb import IceDBv3, CompressionCodec
from icedb.log import S3Client, IceLogIO
from datetime import datetime
import json
from time import time
import sys
from copy import deepcopy

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
    True,
    compression_codec=CompressionCodec.ZSTD
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
    print("============= schema introspection ==================")
    schema = ice.get_schema(example_events)
    schemaJSON = schema.toJSON()
    assert schemaJSON == '{"ts": "BIGINT", "event": "VARCHAR", "user_id": "VARCHAR", "properties": "VARCHAR", "_row_id": "VARCHAR"}'
    print("schema valid")

    print("============= inserting ==================")
    inserted = ice.insert(deepcopy(example_events))
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
    inserted = ice.insert(deepcopy(more_example_events))
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
    print("============= inserting third ==================")
    inserted = ice.insert(deepcopy(more_example_events))
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

    print("============== merging results ==============")

    previous_logs = l1

    # merge fully
    merged_log, new_file, partition, merged_files, meta = ice.merge(max_file_count=2)
    if merged_log is not None:
        print("merged", merged_log, new_file, partition, merged_files)

    # Read the state in
    log = IceLogIO("dan-mbp")
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("=== Current State ===")
    print("schema:", s1)
    print("files:", f1)
    print("tombstones:", t1)
    print("log files:", l1)

    # verify log tombstones and file marker tombstones
    possible_file_tmb = list(filter(lambda x: 'd=2023-06-07' in x, firstInserted + secondInserted + third_inserted))
    print("possible file tombstones:", possible_file_tmb)
    actual_tomb = list(map(lambda x: x.path, filter(lambda x: x.tombstone is not None, f1)))
    print("got actual tombstones:", actual_tomb)
    for actual_t in actual_tomb:
        assert actual_t in possible_file_tmb

    print("possible log tombstones", previous_logs)
    actual_log_tombstones = sorted(map(lambda x: x.path, t1))
    print("actual log tombstones", actual_log_tombstones)
    for log_tmb in actual_log_tombstones:
        assert log_tmb in actual_log_tombstones

    alive_file_paths = list(map(lambda x: x.path, filter(lambda x: x.tombstone is None, f1)))
    print("got actual alive files:", alive_file_paths)
    assert len(alive_file_paths) == 5

    print("got log file len", len(l1))
    assert len(l1) == 4

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

    print("============= tombstone cleaning ==================")

    # Just clean everything
    cleaned_log_files, deleted_log_files, deleted_data_files = ice.tombstone_cleanup(0)

    expected_cleaned_log_files = list(filter(lambda x: "_m_" in x, l1))
    print("expected cleaned log files", expected_cleaned_log_files)
    print("actual cleaned log files", cleaned_log_files)
    assert cleaned_log_files.sort() == expected_cleaned_log_files.sort()

    print("expected deleted log files", actual_log_tombstones)
    print("actual deleted log files", deleted_log_files)
    assert deleted_log_files.sort() == actual_log_tombstones.sort()

    print("expected deleted data files", actual_tomb)
    print("actual deleted data files", deleted_data_files)
    assert deleted_data_files.sort() == actual_tomb.sort()

    # Verify query results are the same
    # Read the state in
    log = IceLogIO("dan-mbp")
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("=== Current State ===")
    print("schema:", s1)
    print("files:", f1)
    print("tombstones:", t1)
    print("log files:", l1)

    assert len(l1) == 2

    alive_file_paths = list(map(lambda x: x.path, filter(lambda x: x.tombstone is None, f1)))
    print("got actual alive files:", alive_file_paths)
    assert len(alive_file_paths) == 5

    dead_file_paths = list(map(lambda x: x.path, filter(lambda x: x.tombstone is not None, f1)))
    print("got actual dead files:", dead_file_paths)
    assert len(dead_file_paths) == 0

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

    print("============== insert hundreds ==============")
    print("this will take a while...")

    s = time()
    for i in range(200):
        ice.insert(deepcopy(example_events))
        sys.stdout.write(f"\rinserted {i+1}")
        sys.stdout.flush()
    print("")
    print("inserted hundreds in", time() - s)

    print("reading in the state")
    s = time()
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("read hundreds in", time()-s)

    print("files", len(f1), "logs", len(l1))
    assert len(l1) == 202
    assert len(f1) == 405

    print("verify expected results")
    s = time()
    alive_files = list(filter(lambda x: x.tombstone is None, f1))
    print(f"got {len(alive_files)} alive files")
    query = "select count(user_id), user_id from read_parquet([{}]) group by user_id order by count(user_id) desc".format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", alive_files)))
    )
    # THIS IS A BAD IDEA NEVER DO THIS IN PRODUCTION
    ice.ddb.execute(query)
    res = ice.ddb.fetchall()
    print(res, "in", time()-s)

    assert res[0][0] == 406
    assert res[1][0] == 203

    print("merging it")
    s = time()
    merged_log, new_file, partition, merged_files, meta = ice.merge(max_file_count=2000, max_file_size=1_000_000_000)
    print(f"merged partition {partition} with {len(merged_files)} files in", time()-s)

    s = time()
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("read post merge state in", time() - s)

    print("files", len(f1), "logs", len(l1))
    assert len(l1) == 203
    assert len(f1) == 406

    print("verify expected results")
    s = time()
    alive_files = list(filter(lambda x: x.tombstone is None, f1))
    print(f"got {len(alive_files)} alive files")
    query = "select count(user_id), user_id from read_parquet([{}]) group by user_id order by count(user_id) desc".format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", alive_files)))
    )
    # THIS IS A BAD IDEA NEVER DO THIS IN PRODUCTION
    ice.ddb.execute(query)
    res = ice.ddb.fetchall()
    print(res, "in", time() - s)

    assert res[0][0] == 406
    assert res[1][0] == 203

    print("merging many more times to verify")
    for i in range(4):
        s = time()
        merged_log, new_file, partition, merged_files, meta = ice.merge(max_file_count=200,
                                                                        max_file_size=1_000_000_000)
        print(f"merged partition {partition} with {len(merged_files)} files in", time() - s)

    s = time()
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("read post merge state in", time() - s)

    print("files", len(f1), "logs", len(l1))
    assert len(l1) == 205
    assert len(f1) == 408

    print("verify expected results")
    s = time()
    alive_files = list(filter(lambda x: x.tombstone is None, f1))
    print(f"got {len(alive_files)} alive files")
    query = "select count(user_id), user_id from read_parquet([{}]) group by user_id order by count(user_id) desc".format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", alive_files)))
    )
    # THIS IS A BAD IDEA NEVER DO THIS IN PRODUCTION
    ice.ddb.execute(query)
    res = ice.ddb.fetchall()
    print(res, "in", time() - s)

    assert res[0][0] == 406
    assert res[1][0] == 203

    print("tombstone clean it")
    s = time()
    cleaned_log_files, deleted_log_files, deleted_data_files = ice.tombstone_cleanup(0)
    print(f"tombstone cleaned {len(cleaned_log_files)} cleaned log files, {len(deleted_log_files)} deleted log files, {len(deleted_data_files)} data files in", time()-s)

    s = time()
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    print("read post tombstone clean state in", time() - s)

    print("verify expected results")
    s = time()
    alive_files = list(filter(lambda x: x.tombstone is None, f1))
    print(f"got {len(alive_files)} alive files")
    query = "select count(user_id), user_id from read_parquet([{}]) group by user_id order by count(user_id) desc".format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", alive_files)))
    )
    # THIS IS A BAD IDEA NEVER DO THIS IN PRODUCTION
    ice.ddb.execute(query)
    res = ice.ddb.fetchall()
    print(res, "in", time() - s)

    assert res[0][0] == 406
    assert res[1][0] == 203

    testS3Proxy = False
    if testS3Proxy:
        print("============= testing s3 proxy ==================")

        s3proxy = S3Client(s3prefix="tenant", s3bucket="testbucket", s3region="us-east-1",
                           s3endpoint="http://localhost:8080",
                           s3accesskey="user", s3secretkey="password")

        iceproxy = IceDBv3(
            part_func,
            ['event', 'ts'],
            format_row,
            "us-east-1",
            "user",
            "password",
            "http://localhost:8080",
            s3proxy,  # this is for getting and reading state
            "dan-mbp",
            True
        )

        query = ("select count(user_id), user_id from read_parquet(['s3://testbucket/**/*.parquet']) group by user_id "
                 "order by count(user_id) desc")
        print("running query", query)
        s = time()
        iceproxy.ddb.execute(query)
        res = iceproxy.ddb.fetchall()
        print(res, "in", time() - s)

        assert res[0][0] == 406
        assert res[1][0] == 203

    # print("============= insert conflicting types ==================")
    # conflicting_out = ice.insert([{
    #     "ts": 1686176939445,
    #     "event": 1,
    #     "user_id": 1.2,
    #     "properties": {
    #         "hey": "ho",
    #         "numtime": 1,
    #         "nested_dict": {
    #             "ee": "fff"
    #         }
    #     }
    # }])
    # try:
    #     # Read the state in
    #     log = IceLogIO("dan-mbp")
    #     s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
    #     print("=== Current State ===")
    #     print("schema:", s1)
    #     print("files:", f1)
    #     print("tombstones:", t1)
    #     print("log files:", l1)
    # except SchemaConflictException as e:
    #     print("Caught schema conflict, this should normally be handled at insert time")

    print("test successful!")
except Exception as e:
    # print('exception:', type(e).__name__, e)
    raise e
finally:
    # ================== Clean up =========================
    clean = False
    if clean:
        s3_files: list[dict] = []
        no_more_files = False
        continuation_token = ""
        while not no_more_files:
            res = s3c.s3.list_objects_v2(
                Bucket=s3c.s3bucket,
                MaxKeys=1000,
                Prefix='/'.join([s3c.s3prefix, '_log']),
                ContinuationToken=continuation_token
            ) if continuation_token != "" else s3c.s3.list_objects_v2(
                Bucket=s3c.s3bucket,
                MaxKeys=1000,
                Prefix='/'.join([s3c.s3prefix, '_log'])
            )
            s3_files += res['Contents']
            no_more_files = not res['IsTruncated']
            if not no_more_files:
                continuation_token = res['NextContinuationToken']
        for file in s3_files:
            s3c.s3.delete_object(
                Bucket=s3c.s3bucket,
                Key=file['Key']
            )
        print(f"deleted {len(s3_files)} files")