from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, Response
from icedb import IceDB
import json
from datetime import datetime
import os
import duckdb
import duckdb.typing as ty
from time import time
from threading import Timer, Semaphore
import tabulate # for markdown printing, and pipreqs to require it

app = Flask(__name__)
insert_interval_seconds = int(os.environ["INSERT_SEC"]) if "INSERT_SEC" in os.environ and os.environ["INSERT_SEC"].isdigit() else 3
merge_interval_seconds = int(os.environ["MERGE_SEC"]) if "MERGE_SEC" in os.environ and os.environ["MERGE_SEC"].isdigit() else 6
row_group_size = int(os.environ["ROW_GROUP_SIZE"]) if "ROW_GROUP_SIZE" in os.environ and os.environ["ROW_GROUP_SIZE"].isdigit() else 10_000

def get_partition_range(table: str, syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    return ['table={}/y={}/m={}/d={}'.format(table, '{}'.format(syear).zfill(4), '{}'.format(smonth).zfill(2), '{}'.format(sday).zfill(2)),
            'table={}/y={}/m={}/d={}'.format(table, '{}'.format(eyear).zfill(4), '{}'.format(emonth).zfill(2), '{}'.format(eday).zfill(2))]

def part_segment(row: dict) -> str:
    rowtime = datetime.fromisoformat(row['receivedAt']) if 'receivedAt' in row else datetime.utcnow()
    # the `table=segment/` prefix makes it effectively the `segment` table
    part = 'table={}/y={}/m={}/d={}'.format(row["table"], '{}'.format(rowtime.year).zfill(4), '{}'.format(rowtime.month).zfill(2), '{}'.format(rowtime.day).zfill(2))
    return part

def format_segment(row: dict) -> dict:
    final_row = {
        "ts": int(datetime.fromisoformat(row['receivedAt']).timestamp()*1000 if 'receivedAt' in row else datetime.utcnow().timestamp()*1000), # convert to ms
        "event": "", # replaced below
        "user_id": row['userId'] if 'userId' in row and row['userId'] != None else row['anonymousId'],
        "anonymous": False if 'userId' in row else True,
        "properties": json.dumps(row["properties"]) if "properties" in row else {},
        "og_payload": json.dumps(row)
    }

    if row['type'] == 'page':
        final_row['event'] = "page.{}".format(row["name"])
    elif row["type"] == "identify":
        final_row['event'] = "identify"
    elif row["type"] == "track":
        final_row["event"] = row["event"]

    return final_row

ice = IceDB(
    partitionStrategy=part_segment,
    sortOrder=['event', 'ts'],
    pgdsn=os.environ["DSN"],
    s3bucket=os.environ["S3_BUCKET"],
    s3region=os.environ["S3_REGION"],
    s3accesskey=os.environ["S3_ACCESS_KEY"],
    s3secretkey=os.environ["S3_SECRET_KEY"],
    s3endpoint=os.environ["S3_ENDPOINT"],
    create_table=os.environ["CREATE_TABLE"] == "1" if "CREATE_TABLE" in os.environ else False,
    formatRow=format_segment,
    duckdb_ext_dir='/app/duckdb_exts',
    unique_row_key='messageId',
    row_group_size=row_group_size
)

# Caching because of duckdb double-triggering with read_parquet. A normal var was causing Unbound access errors
class cache():
    val: any = None
    def get(self):
        return self.val
    def set(self, v):
        self.val = v

c = cache()

def get_files(table: str, syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    part_range = get_partition_range(table, syear, smonth, sday, eyear, emonth, eday)
    return ice.get_files(
        part_range[0],
        part_range[1]
    )

def auth_header() -> bool:
    if "AUTH" not in os.environ:
        return True
    authSecret = os.environ["AUTH"]
    authHeader = request.headers.get('Authorization')
    try:
        return authSecret == authHeader.split('Bearer ')[1]
    except Exception as e:
        return False


@app.route('/hc')
def hello():
    return 'y'

@app.route('/query', methods=['POST'])
def query():
    if not auth_header():
        return 'invalid auth', 401
    
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        j = request.get_json()
    else:
        return 'not json', 400



    ddb = duckdb.connect(":memory:")
    ddb.execute("install httpfs")
    ddb.execute("load httpfs")
    ddb.execute(f"SET s3_region='{os.environ['S3_REGION']}'")
    ddb.execute(f"SET s3_access_key_id='{os.environ['S3_ACCESS_KEY']}'")
    ddb.execute(f"SET s3_secret_access_key='{os.environ['S3_SECRET_KEY']}'")
    ddb.execute(f"SET s3_endpoint='{os.environ['S3_ENDPOINT'].split('://')[1]}'")
    ddb.execute(f"SET s3_use_ssl={'false' if 'http://' in os.environ['S3_ENDPOINT'] else 'true'}")
    ddb.execute("SET s3_url_style='path'")
    ddb.create_function('get_files_bind', get_files, [ty.VARCHAR, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER], list[str])
    ddb.sql('''
        create macro if not exists get_files(tabl:='segment', start_year:=2023, start_month:=1, start_day:=1, end_year:=2023, end_month:=1, end_day:=1) as get_files_bind(tabl, start_year, start_month, start_day, end_year, end_month, end_day)
    ''')
    ddb.sql('''
        create macro if not exists icedb(tabl:='segment', start_year:=2023, start_month:=1, start_day:=1, end_year:=2023, end_month:=1, end_day:=1) as table select * from read_parquet(get_files_bind(tabl, start_year, start_month, start_day, end_year, end_month, end_day), hive_partitioning=1, filename=1)
    ''')
    try:
        if "format" not in j:
            return "need to specify format!", 400
        s = time()*1000
        result = ddb.execute(j['query'])
        print('got query res in', time()*1000-s)
        s = time()*1000
        if j['format'] == "csv":
            result = result.df().to_csv(index=False)
            print("formatted csv in", time()*1000-s)
            return Response(result, content_type='text/csv')
        if j['format'] == "pretty":
            r = result.df().to_markdown(index=False)
            print("formatted pretty in", time()*1000-s)
            return r
        else:
            return "unsupported format!", 400
    except duckdb.IOException as e:
        if "Parquet reader needs at least one file to read" in str(e):
            return "no data in time range!", 404
        else:
            raise e
    except Exception as e:
        raise e

class InsertBuffer():
    map: dict[str, list[str]] = {}
    lock: bool = False
    t: Timer
    sem: Semaphore

    def __init__(self):
        self.t = Timer(insert_interval_seconds, self.insertBatch)
        self.t.start()
        self.sem = Semaphore(1)

    def insertRow(self, table: str, row: dict):
        try:
            self.sem.acquire()
            if table not in self.map:
                self.map[table] = []
            self.map[table].append(row)
        finally:
            self.sem.release()
    
    def insertBatch(self):
        try:
            self.sem.acquire()
            for table in self.map:
                ice.insert(self.map[table])
                print('buffer inserted', len(self.map[table]), 'for', table)
        finally:
            self.map = {}
            self.t.cancel()
            self.t = Timer(insert_interval_seconds, self.insertBatch)
            self.t.start()
            self.sem.release()
    
    def stop(self):
        self.t.cancel()

buf = InsertBuffer()

# Post an event directly
@app.route('/<table>/insert', methods=['POST'])
def insert_segment(table):
    if not auth_header():
        return 'invalid auth', 401
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        j = request.get_json()
        if isinstance(j, dict):
            j["table"] = table # add the table in
            buf.insertRow(table, j)
            # inserted = ice.insert([j])
            # return inserted
            return "buffered"
        if isinstance(j, list):
            # add the table in
            for row in j:
                row["table"] = table
                buf.insertRow(table, row)
            # inserted = ice.insert(j)
            # return inserted
            return "buffered"
        return 'bad JSON!'
    else:
        return 'Content-Type not supported!'

class MergeTimer():
    t: Timer
    sem: Semaphore
    tables = ['segment', 'twitch-ext']

    def __init__(self):
        self.t = Timer(merge_interval_seconds, self.merge)
        self.t.start()
        self.sem = Semaphore(1)
    
    def merge(self):
        try:
            self.sem.acquire()
            for table in self.tables:
                res = merge(table)
                if res > 0:
                    print('merged', res, 'for', table)
        finally:
            self.map = {}
            self.t.cancel()
            self.t = Timer(insert_interval_seconds, self.merge)
            self.t.start()
            self.sem.release()
    
    def stop(self):
        self.t.cancel()

mrg = MergeTimer()

def merge(table: str):
    res = ice.merge_files(10_000_000, maxFileCount=100, partition_prefix=f"table={table}/",
                          custom_merge_query="""
    select
        any_value(user_id) as user_id,
        any_value(event) as event,
        any_value(properties) as properties,
        any_value(og_payload) as og_payload,
        any_value(anonymous) as anonymous,
        any_value(ts) as ts,
        _row_id
    from source_files
    group by _row_id
    """
    )
    return res

@app.route('/<table>/merge', methods=['POST'])
def merge_files(table):
    if not auth_header():
        return 'invalid auth', 401
    return str(merge(table))

def shutdown():
    print('shutting down!')
    buf.stop()
    buf.insertBatch()
    buf.stop()
    mrg.stop()
    ice.close()

if __name__ == '__main__':
    app.run(debug=True if "DEBUG" in os.environ and os.environ['DEBUG'] == '1' else False, port=int(os.environ['PORT']) if "PORT" in os.environ else 8090, host='0.0.0.0')
    shutdown()
