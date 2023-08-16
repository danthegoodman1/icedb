"""
An API that ingests events as JSON, batches them on an interval, and inserts.
It also will merge and tombstone clean on separate intervals.

For a single host setup, besides running Flask in debug mode, this is an otherwise
production-ready setup for the provided events.

Run:
`docker compose up -d`

Then:
`python api-flask.py`

In another terminal try inserting with:
```
curl http://localhost:8090/insert -H "Content-type: application/json" \
-d '{
  "ts": 1686176939445,
  "event": "page_load",
  "user_id": "user_a",
  "properties": {
    "page_name": "Home"
  }
}'
```
(and modify the json as you like for additional inserts)

Then after the batch inserts, query with:
```
curl http://localhost:8090/query
```
"""

from icedb.icedb import IceDBv3, CompressionCodec
from icedb.log import IceLogIO
from datetime import datetime
import json
from time import time
from helpers import get_local_ddb, get_local_s3_client, delete_all_s3
from threading import Timer
from flask import Flask, request
import os


class IceDBBatcher(object):
    """
    Buffers inserted rows into memory and batch inserts them into icedb.

    Runs merge on 10x the insert interval, and tombstone clean on 50x the insert interval.

    Adapted from https://stackoverflow.com/questions/3393612/run-certain-code-every-n-seconds
    """

    def __init__(self, icedb: IceDBv3, insert_interval_sec=3):
        self._timer = None
        self._timer_merge = None
        self._timer_tombstone = None
        self.insert_interval_sec = insert_interval_sec
        self.icedb = icedb
        self.is_running = False
        self.is_running_merge = False
        self.is_running_tombstone = False
        self.start()
        self.rows = []

    def insert(self, rows: list[dict]):
        # just append rows
        self.rows = self.rows + rows

    def _insert(self):
        self.is_running = False
        if len(self.rows) > 0:
            try:
                s = time()
                self.icedb.insert(self.rows)
                print("inserted in", time()-s)
                self.rows = []
            except Exception as e:
                print("caught exception in _insert")
                print(e)
        self.start()

    def _merge(self):
        self.is_running_merge = False
        try:
            merged_log: str | None = ""
            while merged_log is not None:
                print("running merge")
                s = time()
                merged_log, _, _, _, _ = self.icedb.merge()
                if merged_log is not None:
                    print("merged in", time() - s)
                else:
                    print("no files merged")
        except Exception as e:
            print("caught exception in _merge")
            print(e)
        self.start()

    def _tombstone(self):
        self.is_running_tombstone = False
        try:
            print("running tombstone clean")
            s = time()
            cleaned, _, _ = self.icedb.tombstone_cleanup(10_000)
            if len(cleaned) > 0:
                print("tombstone cleaned in", time() - s)
            else:
                print("nothing to tombstone clean")
        except Exception as e:
            print("caught exception in _tombstone")
            print(e)
        self.start()

    def start(self):
        if not self.is_running:
            self._timer = Timer(self.insert_interval_sec, self._insert)
            self._timer.start()
            self.is_running = True
        if not self.is_running_merge:
            self._timer_merge = Timer(self.insert_interval_sec * 10, self._merge)
            self._timer_merge.start()
            self.is_running_merge = True
        if not self.is_running_tombstone:
            self._timer_tombstone = Timer(self.insert_interval_sec * 50, self._tombstone)
            self._timer_tombstone.start()
            self.is_running_tombstone = True

    def stop(self):
        self._timer.cancel()
        self._timer_merge.cancel()
        self._timer_tombstone.cancel()
        self.is_running = False
        self.is_running_merge = False
        self.is_running_tombstone = False


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


ice = IceDBv3(
    part_func,
    ['event', 'ts'],  # We are doing to sort by event, then timestamp of the event within the data part
    format_row,
    "us-east-1",  # This is all local minio stuff
    "user",
    "password",
    "http://localhost:9000",
    s3c,
    "dan-mbp",
    True,  # needed for local minio
    compression_codec=CompressionCodec.ZSTD  # Let's force a higher compression level, default is SNAPPY
)

app = Flask(__name__)


icedb_batcher = IceDBBatcher(ice)


@app.route('/insert', methods=['POST'])
def buffer_rows():
    content_type = request.headers.get('Content-Type')
    if content_type == 'application/json':
        j = request.get_json()
        if isinstance(j, dict):
            icedb_batcher.insert([j])
            return "buffered row"
        if isinstance(j, list):
            icedb_batcher.insert(j)
            return "buffered rows"
        return 'bad JSON!'
    else:
        return 'Content-Type not supported!'


@app.route('/query', methods=['GET'])
def query_rows():
    s1, f1, t1, l1 = IceLogIO("mbp").read_at_max_time(s3c, round(time() * 1000))
    alive_files = list(filter(lambda x: x.tombstone is None, f1))

    # Create a duckdb instance for querying
    ddb = get_local_ddb()

    # Run the query
    query = ("select user_id, count(*), (properties::JSON)->>'page_name' as page "
             "from read_parquet([{}]) "
             "group by user_id, page "
             "order by count(user_id) desc").format(
        ', '.join(list(map(lambda x: "'s3://" + ice.s3c.s3bucket + "/" + x.path + "'", alive_files)))
    )

    # return the result as text
    return str(ddb.sql(query))


if __name__ == '__main__':
    try:
        icedb_batcher.start()
        app.run(debug=True if "DEBUG" in os.environ and os.environ['DEBUG'] == '1' else False,
                port=int(os.environ['PORT']) if "PORT" in os.environ else 8090, host='0.0.0.0')
    finally:
        icedb_batcher.stop()
        delete_all_s3(s3c)
