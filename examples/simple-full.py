"""
Run:
`docker compose up -d`

Then:
`python simple-full.py`
"""

from icedb.log import IceLogIO
from datetime import datetime
from time import time
from helpers import get_local_ddb, get_local_s3_client, delete_all_s3, get_ice
from json import dumps

s3c = get_local_s3_client()


def part_func(row: dict) -> str:
    """
    We'll partition by user_id, date
    """
    row_time = datetime.utcfromtimestamp(row['ts'] / 1000)
    part = f"u={row['user_id']}/d={row_time.strftime('%Y-%m-%d')}"
    return part


ice = get_ice(s3c, part_func)

# Some fake events that we are ingesting
example_events = [
    {
        "ts": 1686176939445,
        "flt": 1.0,
        "event": "page_load",
        "user_id": "user_a",
        "properties": dumps({
            "page_name": "Home"
        })
    }, {
        "ts": 1676126229999,
        "flt": 1.0,
        "event": "page_load",
        "user_id": "user_b",
        "properties": dumps({
            "page_name": "Home"
        })
    }, {
        "ts": 1686176939666,
        "flt": 1.0,
        "event": "page_load",
        "user_id": "user_a",
        "properties": dumps({
            "page_name": "Settings"
        })
    }, {
        "ts": 1686176941445,
        "flt": 1.0,
        "event": "page_load",
        "user_id": "user_a",
        "properties": dumps({
            "page_name": "Home"
        })
    }
]

print("============= inserting events ==================")
inserted = ice.insert(example_events)
firstInserted = list(map(lambda x: x.path, inserted))
print('inserted', firstInserted)

# Read the state in
log = IceLogIO("dan-mbp")
s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))


print("============= running query to see types =============")

# Create a duckdb instance for querying
ddb = get_local_ddb()

# Run the query
query = ("select * "
         "from read_parquet([{}]) ").format(
    ', '.join(list(map(lambda x: "'s3://" + ice.data_s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= running query =============")

# Create a duckdb instance for querying
ddb = get_local_ddb()

# Run the query
query = ("select user_id, count(*), (properties::JSON)->>'page_name' as page "
         "from read_parquet([{}]) "
         "group by user_id, page "
         "order by count(*) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice.data_s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= inserting events ==================")
# insert again to create a second data part, value won't change because we are counting
inserted = ice.insert(example_events)
firstInserted = list(map(lambda x: x.path, inserted))
print('inserted (again)', firstInserted)

print("============= merging =============")
merged_log, new_file, partition, merged_files, meta = ice.merge()
print(f"merged {len(merged_files)} data files from partition {partition}")


print("============= running query =============")

# Read the state in
log = IceLogIO("dan-mbp")
s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))



# Run the query
query = ("select user_id, count(*), (properties::JSON)->>'page_name' as page "
         "from read_parquet([{}]) "
         "group by user_id, page "
         "order by count(*) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice.data_s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= tombstone cleaning =============")
cleaned, deleted_log, deleted_data = ice.tombstone_cleanup(0) # 0 ms so we clean everything
print(f"cleaned {len(cleaned)} log files and deleted {len(deleted_log)} log files and deleted {len(deleted_data)} "
      f"data files")

print("============= running query =============")

# Read the state in
log = IceLogIO("dan-mbp")
s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))



# Run the query
query = ("select user_id, count(*), (properties::JSON)->>'page_name' as page "
         "from read_parquet([{}]) "
         "group by user_id, page "
         "order by count(*) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice.data_s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

delete_all_s3(s3c)