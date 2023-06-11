Run this in clickhouse after it boots with:

```
docker compose -f compose-ch.yml up -d
```

```
docker exec ch bash -c "apt update && apt install python3 python3-pip git libpq-dev -y && pip install git+https://github.com/danthegoodman1/icedb"
```

This is just a basic example of binding a python function. The configuration will have to be changed for actually listing files to pass into `s3()` or `url()` table functions to read the parquet files.

```
docker exec ch clickhouse-client -q "SELECT get_files(2023,2,1, 2023,8,1);"
```
