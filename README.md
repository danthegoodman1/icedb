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

## Pre-installing extensions

DuckDB uses the `httpfs` extension. See how to pre-install it into your runtime here: https://duckdb.org/docs/extensions/overview.html#downloading-extensions-directly-from-s3

and see the `extension_directory` setting: https://duckdb.org/docs/sql/configuration.html#:~:text=PHYSICAL_ONLY-,extension_directory,-Set%20the%20directory with the default of `$HOME/.duckdb/`
