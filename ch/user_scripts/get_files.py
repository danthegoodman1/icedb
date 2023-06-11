#!/usr/bin/python3

import sys
from icedb import IceDB
from datetime import datetime
import json

def partStrat(row: dict) -> str:
    rowTime = datetime.utcfromtimestamp(row['ts']/1000)
    part = 'y={}/m={}/d={}'.format('{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2))
    return part

def format_row(row: dict) -> dict:
    row['properties'] = json.dumps(row['properties']) # convert nested dict to json string
    return row

try:
    ice = IceDB(
        partitionStrategy=partStrat,
        sortOrder=['event', 'ts'],
        pgdsn="postgresql://root@crdb:26257/defaultdb",
        s3bucket="testbucket",
        s3region="us-east-1",
        s3accesskey="Ia3NaZPuGcOEoHIJr6mZ",
        s3secretkey="pS5gWnpb7yErrQzhlhzE62ir4UNbUQ6PGqOvth5d",
        s3endpoint="http://minio:9000",
        formatRow=format_row
    )
except Exception as e:
    print("Value " + str(e), end='')
    sys.stdout.flush()

def get_partition_range(syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    return ['y={}/m={}/d={}'.format('{}'.format(syear).zfill(4), '{}'.format(smonth).zfill(2), '{}'.format(sday).zfill(2)),
            'y={}/m={}/d={}'.format('{}'.format(eyear).zfill(4), '{}'.format(emonth).zfill(2), '{}'.format(eday).zfill(2))]

def get_files(syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    part_range = get_partition_range(syear, smonth, sday, eyear, emonth, eday)
    return ice.get_files(
        part_range[0],
        part_range[1]
    )

if __name__ == '__main__':
    i = 0
    for line in sys.stdin:
        args = line.split('\t')
        print("Value " + line.replace('\t', "-" + str(i) + "++"), end='')
        sys.stdout.flush()
        i+=1
