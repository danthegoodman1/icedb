# ClickHouse and Golang Example

This example shows you how to [create a Golang binary to read from IceDB](/ch/user_scripts/main.go), [bind that to ClickHouse](/ch/functions/get_files_function.xml), and query it from within ClickHouse.

From the root folder, run

```
docker compose up -d
```

Install the requirements:

```
pip install duckdb git+https://github.com/danthegoodman1/icedb
```

Then run the simple example to make sure you have some data in IceDB:

```
python examples/simple.py
```

To run the query against clickhouse just like the DuckDB example in `examples/simple.py`, run:
```
docker exec ch clickhouse-client -q "SELECT sum(JSONExtractInt(properties, 'numtime')), user_id from s3(get_files(2023,2,1, 2023,8,1), 'user', 'password', 'Parquet') where event = 'page_load' group by user_id FORMAT Pretty;"
```

This will show the same results as found in the final query of `examples/simple.py`

You can create a parameterized view for a nicer query experience like:
```
docker exec ch clickhouse-client -q "create view icedb as select * from s3(get_files(toYear({start_date:Date}), toMonth({start_date:Date}), toDate({start_date:Date}), toYear({end_date:Date}), toMonth({end_date:Date}), toDate({end_date:Date})), 'user', 'password', 'Parquet')"

docker exec ch clickhouse-client -q "SELECT sum(JSONExtractInt(properties, 'numtime')), user_id from icedb where start_date = '2023-02-01' and end_date = '2023-08-01 and event = 'page_load' group by user_id FORMAT Pretty;"
```
