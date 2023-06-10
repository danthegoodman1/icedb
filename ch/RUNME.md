Run this in clickhouse after it boots with:

```
docker exec ch bash -c "apt update && apt install python3 -y"
```

This is just a basic example of binding a python function. The configuration will have to be changed for actually listing files to pass into `s3()` or `url()` table functions to read the parquet files.

```
docker exec ch clickhouse-client -q "SELECT test_function_python(toUInt64(2));"
```
