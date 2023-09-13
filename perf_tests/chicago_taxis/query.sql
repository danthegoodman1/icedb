select count()
from s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')

select *
from s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
limit 1000 Format Null

WITH parseDateTime(cast(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
SELECT
    *,
    trip_start_date
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
LIMIT 3


WITH parseDateTime(cast(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS mnth
SELECT count() as cnt, mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
group by mnth
order by mnth desc
Format Null


WITH parseDateTime(cast(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
select * from (
  SELECT
    quantile(toInt64(`Fare`)) as med_fare
    , avg(toInt64(`Fare`)) as avg_fare
    , quantile(toInt64(`Tips`)) as med_tips
    , avg(toInt64(`Tips`)) as avg_tips
    , quantile(toInt64(`Trip Seconds`)) as med_trip_seconds
    , avg(toInt64(`Trip Seconds`)) as avg_trip_seconds
    , quantile(toInt64(`Trip Miles`)) as med_trip_miles
    , avg(toInt64(`Trip Miles`)) as avg_trip_miles
    , date_trunc('month', trip_start_date) as mnth
  FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
  group by mnth
) order by mnth desc
Format Null


SELECT
  count()
  , `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
group by `Payment Type`
order by count() desc
Format Null


WITH parseDateTime(cast(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
SELECT
  count(*)
  , `Payment Type`
  , toMonth(trip_start_date) as mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
where mnth = 8
group by `Payment Type`, mnth
order by count(*) desc
Format Null


WITH parseDateTime(cast(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
SELECT
  count()
  , `Payment Type`
  , toMonth(trip_start_date) as mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
-- leverage the partitioning to only read certain partitions, bigquery does this for us
where trip_start_date >= '2021-01-01'
  and trip_start_date <= '2021-12-31'
group by `Payment Type`, mnth
order by count(*) desc
Format Null