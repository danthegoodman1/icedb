select *
from s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis/_data/**/*.parquet')
limit 1000 Format Null

select count()
from s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis/_data/**/*.parquet')


with toDate(extract(_path, 'd=(\w+)/')) AS trip_start_date
SELECT count() as cnt, date_trunc('month', trip_start_date) as mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis/_data/**/*.parquet', 'Parquet')
group by mnth
order by mnth desc


with toDate(extract(_path, 'd=(\w+)/')) AS trip_start_date
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
  FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis/_data/**/*.parquet', 'Parquet')
  group by mnth
) order by mnth desc


SELECT
  count()
  , payment_type
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis/_data/**/*.parquet', 'Parquet')
group by payment_type
order by count() desc


with toDate(extract(_path, 'd=(\w+)/')) AS trip_start_date
SELECT
  count(*)
  , payment_type
  , toMonth(trip_start_date) as mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis/_data/**/*.parquet', 'Parquet')
where mnth = 8
group by payment_type, mnth
order by count(*) desc


with toDate(extract(_path, 'd=(\w+)/')) AS trip_start_date
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