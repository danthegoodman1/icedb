import duckdb

from icedb import IceDBv3, CompressionCodec
from icedb.log import S3Client


def get_local_ddb():
    ddb = duckdb.connect(":memory:")
    ddb.execute("install httpfs")
    ddb.execute("load httpfs")
    ddb.execute("SET s3_region='us-east-1'")
    ddb.execute("SET s3_access_key_id='user'")
    ddb.execute("SET s3_secret_access_key='password'")
    ddb.execute("SET s3_endpoint='localhost:9000'")
    ddb.execute("SET s3_use_ssl='false'")
    ddb.execute("SET s3_url_style='path'")
    return ddb

def get_local_s3_client():
    return S3Client(s3prefix="tenant", s3bucket="testbucket", s3region="us-east-1",
                      s3endpoint="http://localhost:9000",
                   s3accesskey="user", s3secretkey="password")
def delete_all_s3(s3c: S3Client):
    """
    Deletes all files in the local s3 bucket!
    """
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
        if 'Contents' not in res:
            return
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

def get_ice(s3_client, part_func, format_row):
    return IceDBv3(
    part_func,
    ['event', 'ts'],  # We are doing to sort by event, then timestamp of the event within the data part
    format_row,
    "us-east-1",  # This is all local minio stuff
    "user",
    "password",
    "http://localhost:9000",
    s3_client,
    "dan-mbp",
    True,  # needed for local minio
    compression_codec=CompressionCodec.ZSTD  # Let's force a higher compression level, default is SNAPPY
)