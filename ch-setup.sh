docker compose -f compose-ch.yml up -d

docker exec ch bash -c "apt update && apt install python3 python3-pip git libpq-dev -y && pip install git+https://github.com/danthegoodman1/icedb"

docker exec ch clickhouse-client -q "SELECT get_files(2023,2,1, 2023,8,1) FORMAT Pretty;"
