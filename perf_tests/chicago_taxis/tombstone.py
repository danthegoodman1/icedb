from icedb.icedb import IceDBv3, S3Client
import os
from time import time
from datetime import datetime

def part_func(row: dict) -> str:
    # Normally you should parse this with datetime package and
    # verify it, but we know the data is good, so we'll just short circuit it
    trip_start = row['Trip Start Timestamp']
    if trip_start[4] == '-': # 2015-05-07 20:30:00 UTC
        return '-'.join(trip_start.split('-')[:2])  # 2015-05-07
    else:
        dt = datetime.strptime(trip_start, '%m/%d/%Y %H:%M:%S %p')  # 05/09/2014 07:30:00 PM
        return dt.strftime("%Y-%m")


s3c = S3Client(s3prefix="chicago_taxis_1m", s3bucket=os.getenv("AWS_S3_BUCKET"), s3region=os.getenv("AWS_S3_REGION"),
               s3endpoint=os.getenv("AWS_S3_ENDPOINT"),
               s3accesskey=os.getenv("AWS_KEY_ID"), s3secretkey=os.getenv("AWS_KEY_SECRET"))

ice = IceDBv3(
    part_func,
    ['"Trip Start Timestamp"'],
    os.getenv("AWS_S3_REGION"),
    os.getenv("AWS_KEY_ID"),
    os.getenv("AWS_KEY_SECRET"),
    os.getenv("AWS_S3_ENDPOINT"),
    s3c,
    "local-test"
)
start = time()
while True:
    s = time()
    _, _, deleted_data = ice.tombstone_cleanup(0)
    if deleted_data != 0:
        break
    print(f"Tombstone cleaned {len(deleted_data)} data files in {time()-s} seconds")
print(f"done in {time()-start} seconds")