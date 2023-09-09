import chdb, os

def query(q: str):
    """
    Queries all parquet files through the IceDB S3 proxy, uses a constant virtual bucket
    """
    res = chdb.query(q.replace("tbl", f"""
    from s3('{os.getenv("S3_PROXY_URL")}/fake_bucket/**/*.parquet', 'Parquet')
    """))
    print(f"""
    
    
    ```sql
    {q}
    ```
    
    Time: {res.elapsed()}
    Rows: {res.rows_read()}
    Bytes: {res.bytes_read()}
    """)

# toDate(extract(_path, 'd=(\\w+)/')) AS trip_start_date

query("""
select *
from tbl
limit 1000
""")

query("""
with toDate(extract(_path, 'd=(\\w+)/')) AS trip_start_date
SELECT count() as cnt, date_trunc('month', trip_start_date) as mnth
FROM tbl
group by mnth
order by mnth desc
""")

query("""
with toDate(extract(_path, 'd=(\\w+)/')) AS trip_start_date
select * from (
  SELECT
    quantile(fare) as med_fare
    , avg(fare) as avg_fare
    , quantile(tips) as med_tips
    , avg(tips) as avg_tips
    , quantile(trip_seconds) as med_trip_seconds
    , avg(trip_seconds) as avg_trip_seconds
    , quantile(trip_miles) as med_trip_miles
    , avg(trip_miles) as avg_trip_miles
    , date_trunc(trip_start_date, MONTH) as mnth
  FROM tbl
  group by mnth
) order by mnth desc
""")

query("""
SELECT
  count()
  , payment_type
FROM tbl
group by payment_type
order by count() desc
""")

query("""
with toDate(extract(_path, 'd=(\\w+)/')) AS trip_start_date
SELECT
  count(*)
  , payment_type
  , toMonth(trip_start_date) as mnth
FROM tbl
where mnth = 8
group by payment_type, mnth
order by count(*) desc
""")

query("""
with toDate(extract(_path, 'd=(\\w+)/')) AS trip_start_date
SELECT
  count(*)
  , payment_type
  , toMonth(trip_start_date) as mnth
FROM `tangia-prod.taxis_us.taxi_trips`
-- leverage the partitioning to only read certain partitions, bigquery does this for us
where trip_start_date >= '2021-01-01' 
  and trip_start_date <= '2021-12-31'
group by payment_type, mnth
order by count(*) desc
""")