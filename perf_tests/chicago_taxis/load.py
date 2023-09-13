from icedb.icedb import IceDBv3, S3Client
import csv, os
from time import time
from datetime import datetime

csv_headers = [
    'Trip ID',
    'Taxi ID',
    'Trip Start Timestamp',
    'Trip End Timestamp',
    'Trip Seconds',
    'Trip Miles',
    'Pickup Census Tract',
    'Dropoff Census Tract',
    'Pickup Community Area',
    'Dropoff Community Area',
    'Fare',
    'Tips',
    'Tolls',
    'Extras',
    'Trip Total',
    'Payment Type',
    'Company',
    'Pickup Centroid Latitude',
    'Pickup Centroid Longitude',
    'Pickup Centroid Location',
    'Dropoff Centroid Latitude',
    'Dropoff Centroid Longitude',
    'Dropoff Centroid  Location'
]

flush_limit = 1_000_000


def part_func(row: dict) -> str:
    # Normally you should parse this with datetime package and
    # verify it, but we know the data is good, so we'll just short circuit it
    trip_start = row['Trip Start Timestamp']
    if trip_start[4] == '-':  # 2015-05-07 20:30:00 UTC
        return "d=" + '-'.join(trip_start.split('-')[:2])  # 2015-05-07
    else:
        dt = datetime.strptime(trip_start, '%m/%d/%Y %H:%M:%S %p')  # 05/09/2014 07:30:00 PM
        return dt.strftime("d=%Y-%m")


s3c = S3Client(s3prefix="chicago_taxis_1m", s3bucket=os.getenv("AWS_S3_BUCKET"), s3region=os.getenv("AWS_S3_REGION"),
               s3endpoint=os.getenv("AWS_S3_ENDPOINT"),
               s3accesskey=os.getenv("AWS_KEY_ID"), s3secretkey=os.getenv("AWS_KEY_SECRET"))

ice = IceDBv3(
    part_func,
    ['"Trip Start Timestamp"'],  # We are doing to sort by event, then timestamp of the event within the data part
    os.getenv("AWS_S3_REGION"),  # This is all local minio stuff
    os.getenv("AWS_KEY_ID"),
    os.getenv("AWS_KEY_SECRET"),
    os.getenv("AWS_S3_ENDPOINT"),
    s3c,
    "local-test"
)

# Let's create a row buffer to batch inserts into icedb
row_buf = []


def flush_row_buf():
    s = time()
    files = ice.insert(row_buf)
    print(f"flushed {len(row_buf)} rows and {len(files)} files in {time() - s} seconds")


start = time()

# Open the csv file for reading
with open('chicago_taxis.csv') as csvfile:
    lr = csv.reader(csvfile, delimiter=',')
    next(lr, None)  # skip headers
    for row in lr:
        # convert timestamp to unix seconds
        d = dict(zip(csv_headers, row))  # convert to a dict with the CSV headers as keys
        trip_start: str = d['Trip Start Timestamp']
        try:
            if trip_start[4] == '-':  # 2015-05-07 20:30:00 UTC
                dt = datetime.strptime(trip_start, '%Y-%m-%d %H:%M:%S %Z')
                d['Trip Start Timestamp'] = int(dt.timestamp() * 1000)
            else:
                dt = datetime.strptime(trip_start, '%m/%d/%Y %H:%M:%S %p')  # 05/09/2014 07:30:00 PM
                d['Trip Start Timestamp'] = int(dt.timestamp() * 1000)
        except Exception as e:
            print('exception', e)
            print(d)
        row_buf.append(d)
        if len(row_buf) >= flush_limit:
            flush_row_buf()
            row_buf = []

    if len(row_buf) > 0:
        print(f'performing a final flush of {len(row_buf)} rows')
        flush_row_buf()
        row_buf = []

print(f"done in {time() - start} seconds")
