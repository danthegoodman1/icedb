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

## Pre-installing extensions

DuckDB uses the `httpfs` extension. See how to pre-install it into your runtime here: https://duckdb.org/docs/extensions/overview.html#downloading-extensions-directly-from-s3

and see the `extension_directory` setting: https://duckdb.org/docs/sql/configuration.html#:~:text=PHYSICAL_ONLY-,extension_directory,-Set%20the%20directory with the default of `$HOME/.duckdb/`
