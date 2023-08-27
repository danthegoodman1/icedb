"""
This example verifies the schema before inserting to ensure that the data does not get corrupted.

In practice, you will want to cache the schema in the ingestion workers and when ever there is a change, lookup from
some central data store that supports serializable transactions (Postgres, CockroachDB, FoundationDB, etc.) where you
can lock the schema row and update it if the new schema does not break, otherwise you should drop and/or quarantine
the violating rows for manual review.

Run:
`docker compose up -d`

Then:
`python simple-full.py`
"""

from icedb.icedb import IceDBv3, CompressionCodec
from icedb.log import IceLogIO, NoLogFilesException, Schema, SchemaConflictException
from datetime import datetime
import json
from time import time
from helpers import get_local_ddb, get_local_s3_client, delete_all_s3, get_ice

s3c = get_local_s3_client()


def part_func(row: dict) -> str:
    """
    We'll partition by user_id, date
    """
    row_time = datetime.utcfromtimestamp(row['ts'] / 1000)
    part = f"u={row['user_id']}/d={row_time.strftime('%Y-%m-%d')}"
    return part


def format_row(row: dict) -> dict:
    """
    We can take the row as-is, except let's make the properties a JSON string for safety
    """
    row['properties'] = json.dumps(row['properties'])  # convert nested dict to json string
    return row


ice = get_ice(s3c, part_func, format_row)

# Some fake events that we are ingesting
example_events = [
    {
        "ts": 1686176939445,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Home"
        }
    }, {
        "ts": 1676126229999,
        "event": "page_load",
        "user_id": "user_b",
        "properties": {
            "page_name": "Home"
        }
    }, {
        "ts": 1686176939666,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Settings"
        }
    }, {
        "ts": 1686176941445,
        "event": "page_load",
        "user_id": "user_a",
        "properties": {
            "page_name": "Home"
        }
    }
]

violating_events = [
    {
        "event": "page_load",
        "user_id": 111, # this will break it
        "properties": {
            "page_name": "Home"
        }
    }
]

log = IceLogIO("test")

def check_schema_conflicts(old: Schema, new: Schema):
    """
    Compares 2 schemas and determines whether the new schema conflicts with the old schema
    """
    for col in new.columns():
        if col in old:
            if old[col] != new[col]:
                raise SchemaConflictException(col, old[col], new[col])

print("============= inserting events ==================")
# Get initial schema
try:
    s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
except NoLogFilesException as e:
    print("no log files yet, we will make a blank schema")

schema = Schema()

# First lets check if the schema is different
new_schema = ice.get_schema(example_events)
# You'd probably store a hash in practice, so it's smaller in memory
if new_schema.toJSON() is not schema.toJSON():
    # Let's check if it's safe
    check_schema_conflicts(schema, new_schema)

inserted = ice.insert(example_events)
firstInserted = list(map(lambda x: x.path, inserted))
print('inserted', firstInserted)

# Read the state in (and get the new schema)
schema, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
alive_files = list(filter(lambda x: x.tombstone is None, f1))

print("============= running query =============")

# Create a duckdb instance for querying
ddb = get_local_ddb()

# Run the query
query = ("select user_id, count(*), (properties::JSON)->>'page_name' as page "
         "from read_parquet([{}]) "
         "group by user_id, page "
         "order by count(user_id) desc").format(
    ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", alive_files)))
)
print(ddb.sql(query))

print("============= inserting schema-violating events ==================")

try:
    # First let's check the schemas
    new_schema = ice.get_schema(violating_events)
    if new_schema.toJSON() is not schema.toJSON():
        # Let's check for conflicts
        check_schema_conflicts(schema, new_schema)

    # insert again to create a second data part, value won't change because we are counting
    inserted = ice.insert(violating_events)
    raise Exception("Inserted violating events!")
except SchemaConflictException as e:
    print("caught expected schema violation exception, so we aborted insert:", e)
finally:
    delete_all_s3(s3c)
