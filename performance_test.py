import math
from icedb.icedb import IceDBv3, CompressionCodec
from icedb.log import S3Client
from time import time

num_rows = 6_000_000
num_parts = 16

s3c = S3Client(s3prefix="tenant", s3bucket="testbucket", s3region="us-east-1", s3endpoint="http://localhost:9000",
               s3accesskey="user", s3secretkey="password")


def part_func(row: dict) -> str:
    row_time = math.floor(row['ts'])
    part = f"{row_time % num_parts}"  # do 100 partitions
    return part


ice = IceDBv3(
    part_func,
    ['event', 'ts'],
    "us-east-1",
    "user",
    "password",
    "http://localhost:9000",
    s3c,
    "dan-mbp",
    s3_use_path=True,
    compression_codec=CompressionCodec.ZSTD
)

try:
    print("============= testing insert performance ==================")
    print(f"making {num_rows} fake rows...")
    rows = []
    s = time()
    for i in range(num_rows):
        rows.append({
            "ts": math.floor(time()*1000),
            "event": "page_load",
            "user_id": "a",
            "hey": "ho",
            "num": 1,
            "ee": "fff"
        })
    print(f"made {num_rows} rows in {time()-s}")
    s = time()
    inserted = ice.insert(rows)
    print(f"Inserted in {time()-s}")

    print("\n============= testing insert performance with _partition ==================")
    ice = IceDBv3(
        part_func,
        ['event', 'ts'],
        "us-east-1",
        "user",
        "password",
        "http://localhost:9000",
        s3c,
        "dan-mbp",
        s3_use_path=True,
        compression_codec=CompressionCodec.ZSTD,
    )
    print(f"making {num_rows} fake rows...")
    rows = []
    s = time()
    for i in range(num_rows):
        rows.append({
            "ts": math.floor(time() * 1000),
            "event": "page_load",
            "user_id": "a",
            "hey": "ho",
            "num": 1,
            "ee": "fff",
            "_partition": f"{math.floor(time() * 1000) % num_parts}"
        })
    print(f"made {num_rows} rows in {time() - s}")
    s = time()
    inserted = ice.insert(rows)
    print(f"Inserted in {time() - s}")

    print("\n============= testing insert performance with _partition (no delete) ==================")
    ice = IceDBv3(
        part_func,
        ['event', 'ts'],
        "us-east-1",
        "user",
        "password",
        "http://localhost:9000",
        s3c,
        "dan-mbp",
        s3_use_path=True,
        compression_codec=CompressionCodec.ZSTD,
        preserve_partition=True
    )
    print(f"making {num_rows} fake rows...")
    rows = []
    s = time()
    for i in range(num_rows):
        rows.append({
            "ts": math.floor(time() * 1000),
            "event": "page_load",
            "user_id": "a",
            "hey": "ho",
            "num": 1,
            "ee": "fff",
            "_partition": f"{math.floor(time() * 1000) % num_parts}"
        })
    print(f"made {num_rows} rows in {time() - s}")
    s = time()
    inserted = ice.insert(rows)
    print(f"Inserted in {time() - s}")

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
