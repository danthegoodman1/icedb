from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, Response
from icedb import IceDB
import json
from datetime import datetime
import os
import duckdb
import duckdb.typing as ty

app = Flask(__name__)

def get_partition_range(table: str, syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    return ['table={}/y={}/m={}/d={}'.format(table, '{}'.format(syear).zfill(4), '{}'.format(smonth).zfill(2), '{}'.format(sday).zfill(2)),
            'table={}/y={}/m={}/d={}'.format(table, '{}'.format(eyear).zfill(4), '{}'.format(emonth).zfill(2), '{}'.format(eday).zfill(2))]

def auth_header() -> bool:
    if "AUTH" not in os.environ:
        return True
    authSecret = os.environ["AUTH"]
    authHeader = request.headers.get('Authorization')
    try:
        return authSecret == authHeader.split('Bearer ')[1]
    except Exception as e:
        return False

def createSegmentIce():
    return IceDB(
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
        unique_row_key='messageId'
    )

def get_files(table: str, syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    part_range = get_partition_range(table, syear, smonth, sday, eyear, emonth, eday)
    print('part range', part_range)
    ice = createSegmentIce()
    res = ice.get_files(
        part_range[0],
        part_range[1]
    )
    print('got files', res)
    ice.close()
    return res


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
    ddb.execute("SET s3_region='us-east-1'")
    ddb.execute("SET s3_access_key_id='user'")
    ddb.execute("SET s3_secret_access_key='password'")
    ddb.execute("SET s3_endpoint='localhost:9000'")
    ddb.execute("SET s3_use_ssl=false")
    ddb.execute("SET s3_url_style='path'")
    ddb.create_function('get_files', get_files, [ty.VARCHAR, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER, ty.INTEGER], list[str])
    ddb.sql('''
        create macro if not exists get_f(tabl:='segment', start_year:=2023, start_month:=1, start_day:=1, end_year:=2023, end_month:=1, end_day:=1) as get_files(tabl, start_year, start_month, start_day, end_year, end_month, end_day)
    ''')
    ddb.sql('''
        create macro if not exists icedb(tabl:='segment', start_year:=2023, start_month:=1, start_day:=1, end_year:=2023, end_month:=1, end_day:=1) as table select * from read_parquet(get_files(tabl, start_year, start_month, start_day, end_year, end_month, end_day), hive_partitioning=1, filename=1)
    ''')
    try:
        result = ddb.sql(j['query'])
        if "format" not in j:
            return str(result)
        else:
            if j['format'] == "csv":
                result = result.df().to_csv(index=False)
                return Response(result, content_type='text/csv')
            if j['format'] == "pretty":
                return str(result)
        return result
    except duckdb.IOException as e:
        if "Parquet reader needs at least one file to read" in str(e):
            # TODO: give empty CSV with known headers
            return "no data in time range!", 404
    except Exception as e:
        raise e

def part_segment(row: dict) -> str:
    rowtime = datetime.fromisoformat(row['timestamp'])
    # the `table=segment/` prefix makes it effectively the `segment` table
    part = 'table=segment/y={}/m={}/d={}'.format('{}'.format(rowtime.year).zfill(4), '{}'.format(rowtime.month).zfill(2), '{}'.format(rowtime.day).zfill(2))
    return part

def format_segment(row: dict) -> dict:
    final_row = {
        "ts": datetime.fromisoformat(row['timestamp']).timestamp()*1000, # convert to ms
        "event": "", # replaced below
        "user_id": row['userId'],
        "og_payload": json.dumps(row)
    }

    if row['type'] == 'page':
        final_row['event'] = "page.{}".format(row["name"])
    elif row["type"] == "identify":
        final_row['event'] = "identify"
    elif row["type"] == "track":
        final_row["event"] = row["event"]

    return final_row

def createSegmentIce():
    return IceDB(
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
        unique_row_key='messageId'
    )

# Post a segment event directly
@app.route('/segment/insert', methods=['POST'])
def insert_segment():
    if not auth_header():
        return 'invalid auth', 401
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        j = request.get_json()
        ice = createSegmentIce()
        if isinstance(j, dict):
            inserted = ice.insert([j])
            ice.close()
            return inserted
        if isinstance(j, list):
            inserted = ice.insert(j)
            ice.close()
            return inserted
        return 'bad JSON!'
    else:
        return 'Content-Type not supported!'

@app.route('/segment/merge', methods=['POST'])
def merge_files():
    if not auth_header():
        return 'invalid auth', 401
    ice = createSegmentIce()
    res = ice.merge_files(10_000_000, partition_prefix='table=segment/')
    ice.close()
    return str(res)


if __name__ == '__main__':
    app.run(debug=True if "DEBUG" in os.environ and os.environ['DEBUG'] == '1' else False, port=int(os.environ['PORT']) if "PORT" in os.environ else 8090)
