from icedb import IceDB
from datetime import datetime

def partStrat(row: dict) -> str:
    rowTime = datetime.utcfromtimestamp(row['ts']/1000)
    part = 'y={}/m={}/d={}'.format('{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2))
    return part


ice = IceDB(
    partitionStrategy=partStrat,
    sortOrder=['event', 'ts'],
    pgdsn="postgresql://root@localhost:26257/defaultdb",
    s3bucket="testbucket",
    s3region="us-east-1",
    s3accesskey="Ia3NaZPuGcOEoHIJr6mZ",
    s3secretkey="pS5gWnpb7yErrQzhlhzE62ir4UNbUQ6PGqOvth5d",
    s3endpoint="http://localhost:9000"
)

def get_partition_range(syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    return ['y={}/m={}/d={}'.format('{}'.format(syear).zfill(4), '{}'.format(smonth).zfill(2), '{}'.format(sday).zfill(2)),
            'y={}/m={}/d={}'.format('{}'.format(eyear).zfill(4), '{}'.format(emonth).zfill(2), '{}'.format(eday).zfill(2))]

def get_files(syear: int, smonth: int, sday: int, eyear: int, emonth: int, eday: int) -> list[str]:
    part_range = get_partition_range(syear, smonth, sday, eyear, emonth, eday)
    return ice.get_files(
        part_range[0],
        part_range[1]
    )


# print(get_partition_range(2023,1,1, 2023,8,1))

example_events = [{
    "ts": 1686176939445,
    "event": "page_load",
    "user_id": "a",
    "properties": {
        "hey": "ho",
        "numtime": 123,
        "nested_dict": {
            "ee": "fff"
        }
    }
}, {
    "ts": 1676126229999,
    "event": "page_load",
    "user_id": "b",
    "properties": {
        "hey": "hoergergergrergereg",
        "numtime": 933,
        "nested_dict": {
            "ee": "fff"
        }
    }
}, {
    "ts": 1686176939666,
    "event": "something_else",
    "user_id": "a",
    "properties": {
        "hey": "ho",
        "numtime": 222,
        "nested_dict": {
            "ee": "fff"
        }
    }
}]

inserted = ice.insert(example_events)
print('inserted', inserted)

print('got files', len(get_files(2020,1,1, 2024,8,1)))

merged = ice.merge_files(100_000)
print('merged', merged)

print('got files', len(get_files(2020,1,1, 2024,8,1)))

merged = ice.merge_files(100_000)
print('merged', merged)

print('got files', len(get_files(2020,1,1, 2024,8,1)))
