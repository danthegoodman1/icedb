# IceDB

IceDB is a truly serverless OLAP/data lake hybrid in-process database framework optimized for event-based data. It combines DuckDB, Parquet files, and S3 storage to fully decouple storage and compute, while only paying for compute during query execution time.

_Massive WIP_

See https://blog.danthegoodman.com/icedb-v2

## Usage

```
pip install git+https://github.com/danthegoodman1/icedb
```

```python
from icedb import IceDB

ice = IceDB(...)
```

### `partitionStrategy`

Function that takes in an `dict`, and returns a `str`. How the partition is determined from the row dict.

Example:

```python
from datetime import datetime

def partStrat(row: dict) -> str:
    rowTime = datetime.utcfromtimestamp(row['timestamp']/1000) # timestamp is in ms
    return 'y={}/m={}/d={}'.format('{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2))

ice = IceDB(partitionStrategy=partStrat, sortOrder=['event', 'timestamp'])
```

### `sortOrder`

Defines the order of top-level keys in the row dict that will be used for sorting inside of the parquet file.

Example:

```python
['event', 'timestamp']
```

### `formatRow`

Format row is the function that will determine how a row is finally formatted to be inserted. This is where you would want to flatten JSON so that the data is not corrupted.

**It's crucial that you flatten the row and do not have nested objects**

Example:

```python
def format_row(row: dict) -> dict:
    row['properties'] = json.dumps(row['properties']) # convert nested dict to json string
    return row
```

## Pre-installing extensions

DuckDB uses the `httpfs` extension. See how to pre-install it into your runtime here: https://duckdb.org/docs/extensions/overview.html#downloading-extensions-directly-from-s3

and see the `extension_directory` setting: https://duckdb.org/docs/sql/configuration.html#:~:text=PHYSICAL_ONLY-,extension_directory,-Set%20the%20directory with the default of `$HOME/.duckdb/`

## Concurrent merges

Concurrent merges won't break anything due to the isolation level employed in the meta store transactions, however there is a chance that competing merges can result in conflicts, and when one is detected the conflicting merge will exit. Instead, you can choose to immediately call `merge` again (or with a short, like 5 seconds) if you successfully merged files to ensure that lock contention stays low.

However concurrent merges in opposite directions is highly suggested.

For example in the use case where a partition might look like `y=YYYY/m=MM/d=DD` then you should merge in `DESC` order frequently (say once every 15 seconds). This will keep the hot partitions more optimized so that queries on current data don't get too slow. These should have smaller file count and size requirements so they can be fast, and reduce the lock time of files in the meta store.

You should run a second, slower merge internal in `ASC` order that fully optimizes older partitions. These merges can be much large in file size and count, as they are less likely to conflict with active queries. Say this is run every 5 or 10 minutes.
