# Performance Test for Chicago Taxis Dataset

BigQuery dataset: https://console.cloud.google.com/marketplace/product/city-of-chicago-public-data/chicago-taxi-trips?project=tangia-prod

Raw Data Location: https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew


## Setting up BigQuery

For some reason, Google has decided not to partition this data set, so this means we need to copy it into our own. 
At 75.75GB (at time of writing), that should cost <$0.50 to select that data into a new table.

```sql
CREATE TABLE
`project.dataset.table` -- replace your values!
PARTITION BY
  DATE_TRUNC(trip_start_timestamp, MONTH)
AS select * from `bigquery-public-data.chicago_taxi_trips.taxi_trips`
;
```

This took 43 seconds to run for me.

Here are the stats for the bytes:

```
Number of rows 208,943,621
Total logical bytes 75.75 GB
Active logical bytes 75.75 GB
Long term logical bytes 0 B
Total physical bytes 16.28 GB
Active physical bytes 16.28 GB
Long term physical bytes 0 B
Time travel physical bytes
```

## Setting up IceDB


## Comparing Queries

### BigQuery

```sql
SELECT * FROM `tangia-prod.taxis_us.taxi_trips` limit 1000
```

Time: 352ms
Bytes processed: 75.75 GB
Bytes billed: 75.75 GB

_wtf google???!! charge me the whole dataset to select the first 1k!?_

```sql
select * from (
  SELECT count(*) as cnt, date_trunc(trip_start_timestamp, MONTH) as mnth
  FROM `tangia-prod.taxis_us.taxi_trips`
  group by date_trunc(trip_start_timestamp, MONTH)
) order by mnth desc
;
```

Yeah, I had to do the nested query because `order by date_trunc(trip_start_timestamp, MONTH) desc` is errors with: 
`ORDER 
BY clause expression references column trip_start_timestamp which is neither grouped nor aggregated at [4:21]`.. 
nice...

This should be able to count efficiently using at most some overhead reads from a single column.

Time: 929ms
Bytes processed: 1.56 GB
Bytes billed: 1.56 GB

```sql
select * from (
  SELECT
    APPROX_QUANTILES(fare, 100)[OFFSET(50)] as med_fare
    , avg(fare) as avg_fare
    , APPROX_QUANTILES(tips, 100)[OFFSET(50)] as med_tips
    , avg(tips) as avg_tips
    , APPROX_QUANTILES(trip_seconds, 100)[OFFSET(50)] as med_trip_seconds
    , avg(trip_seconds) as avg_trip_seconds
    , APPROX_QUANTILES(trip_miles, 100)[OFFSET(50)] as med_trip_miles
    , avg(trip_miles) as avg_trip_miles
    , date_trunc(trip_start_timestamp, MONTH) as mnth
  FROM `tangia-prod.taxis_us.taxi_trips`
  group by date_trunc(trip_start_timestamp, MONTH)
) order by mnth desc
;
```

This will iterate over the 4 columns only.

Time: 1 sec
Bytes processed: 7.77 GB
Bytes billed: 7.77 GB

```sql
SELECT
  count(*)
  , payment_type
FROM `tangia-prod.taxis_us.taxi_trips`
group by payment_type
order by count(*) desc
```

This should only iterate over the single column.

Time: 742ms
Bytes processed: 1.73 GB
Bytes billed: 1.73 GB

```sql
SELECT
  count(*)
  , payment_type
  , extract(month from trip_start_timestamp)
FROM `tangia-prod.taxis_us.taxi_trips`
where extract(month from trip_start_timestamp) = 8
group by payment_type, extract(month from trip_start_timestamp)
order by count(*) desc
```

This should only need to iterate over a single column within a single partition.

Time: 816ms
Bytes processed: 3.29 GB
Bytes billed: 3.29 GB

It seems that the query engine is not optimized to handle extraction of dates from the partition key.

```sql
SELECT
  count(*)
  , payment_type
  , extract(month from trip_start_timestamp)
FROM `tangia-prod.taxis_us.taxi_trips`
where date_trunc(trip_start_timestamp, month) >= '2021-01-01'
  and date_trunc(trip_start_timestamp, month) <= '2021-12-31'
group by payment_type, extract(month from trip_start_timestamp)
order by count(*) desc
```

This will read a single column from a subset of the partitions.

Time: 600ms
Bytes processed: 63.69 MB
Bytes billed: 64 MB

### IceDB

Turns out the downloaded data set is in pretty random order... despite that, it seems that with concurrent uploads 
it's sub-linear time to upload high partitions counts. The following snippet was from a 32 vCPU machine using 32
max_threads (16vCPU was identical performance):

```
flushed 100000 rows and 19 files in 2.2882542610168457 seconds
flushed 100000 rows and 16 files in 2.370692729949951 seconds
flushed 100000 rows and 12 files in 3.20159649848938 seconds
flushed 100000 rows and 17 files in 2.4076621532440186 seconds
flushed 100000 rows and 14 files in 2.456570863723755 seconds
flushed 100000 rows and 14 files in 2.3238229751586914 seconds
flushed 100000 rows and 15 files in 2.4138472080230713 seconds
flushed 100000 rows and 11 files in 2.7027711868286133 seconds
flushed 100000 rows and 9 files in 2.4959611892700195 seconds
flushed 100000 rows and 15 files in 2.312371253967285 seconds
performing a final flush of 12921 rows
flushed 12921 rows and 10 files in 0.9287457466125488 seconds
done in 6661.757550239563 seconds
```

Bumped to batches of 1M, we can see a significant performance increase:
```
flushed 1000000 rows and 38 files in 11.008766889572144 seconds
flushed 1000000 rows and 12 files in 12.015792608261108 seconds
flushed 1000000 rows and 9 files in 13.039586782455444 seconds
flushed 1000000 rows and 19 files in 16.860508918762207 seconds
flushed 1000000 rows and 20 files in 12.093096017837524 seconds
flushed 1000000 rows and 26 files in 12.574611902236938 seconds
performing a final flush of 512921 rows
flushed 512921 rows and 24 files in 6.469081163406372 seconds
done in 5156.894633293152 seconds
```

The larger the batches, the more efficient uploads are as compression becomes more effective. The bottlenecks at the 
time of writing roughly in order of slowness are:

- Calculating the partition for each row (this is not parallelized)
- Adding `_row_id` if it doesn't already exist
- Converting the rows into pyarrow tables so that DuckDB can zero-copy upload them to S3

The upload to S3 actually is the fastest step many times over!

It performs shockingly well at high partition counts. In reality inserts should never touch more than a few 
partitions. If doing <10 partitions and using a custom minio cluster, this size could easily push millions of 
inserts per second. Double up the core count to achieve that with S3.

I won't get hung up on this any longer as I know there are many optimizations to make it many times faster, and that 
you can just add more instances of IceDB and parallelize even more (only real limit is S3 rate limit), but it's 
probably faster than you can send to bigquery anyway through their rows-like API :P. Overall I'm quite happy with 
this, and am already planning lots of optimizations!

For queries, we'll use the 1M batches

#### Queries (pre merge, direct file access, m7a.32xlarge 128vCPU)

Sanity check count:

```
SELECT
    count(),
    uniq(_file)
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')

Query id: 29e26b39-ac69-4ca6-a147-47ea6afb32fa

┌───count()─┬─uniq(_file)─┐
│ 209512921 │        4650 │
└───────────┴─────────────┘

1 row in set. Elapsed: 2.699 sec. Processed 209.51 million rows, 0.00 B (77.62 million rows/s., 0.00 B/s.)
Peak memory usage: 936.95 MiB.
```

A little more data than BigQuery (BQ updates every month IIRC), and 4.6k data files... that's a lot, merging should 
have a substantial benefit!


```
SELECT *
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
LIMIT 1000
FORMAT `Null`

Query id: 303402db-19a7-4fc1-929d-411ed5054ae6

Ok.

0 rows in set. Elapsed: 0.371 sec. Processed 1.20 thousand rows, 633.06 KB (3.23 thousand rows/s., 1.71 MB/s.)
Peak memory usage: 9.87 MiB.
```

Hmmmm... 633.06KB with ClickHouse vs. 75GB from BigQuery...

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS mnth
SELECT
    count() AS cnt,
    mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet', 'Parquet')
GROUP BY mnth
ORDER BY mnth DESC
FORMAT `Null`

Query id: 3bbc603e-e9aa-485e-9d76-80a822d15506

Ok.

0 rows in set. Elapsed: 1.718 sec. Processed 209.51 million rows, 0.00 B (121.97 million rows/s., 0.00 B/s.)
Peak memory usage: 701.83 MiB.
```

0B vs 1.56GB on BigQuery...

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
SELECT *
FROM
(
    SELECT
        quantile(toInt64(Fare)) AS med_fare,
        avg(toInt64(Fare)) AS avg_fare,
        quantile(toInt64(Tips)) AS med_tips,
        avg(toInt64(Tips)) AS avg_tips,
        quantile(toInt64(`Trip Seconds`)) AS med_trip_seconds,
        avg(toInt64(`Trip Seconds`)) AS avg_trip_seconds,
        quantile(toInt64(`Trip Miles`)) AS med_trip_miles,
        avg(toInt64(`Trip Miles`)) AS avg_trip_miles,
        date_trunc('month', trip_start_date) AS mnth
    FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet', 'Parquet')
    GROUP BY mnth
)
ORDER BY mnth DESC
FORMAT `Null`

Query id: e409b8a0-f022-4c17-b7c8-58c5308b6455

Ok.

0 rows in set. Elapsed: 4.564 sec. Processed 209.51 million rows, 11.36 GB (45.91 million rows/s., 2.49 GB/s.)
Peak memory usage: 1.21 GiB.
```

BigQuery wins by a few GB here on data read, that's probably due to the massive row groups I used on upload (112k), 
more granular row groups would perform a lot better, which we will observe when merged.

```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`

Query id: 68879e6b-33ad-4a86-92db-429ba1d99866

Ok.

0 rows in set. Elapsed: 2.704 sec. Processed 209.51 million rows, 3.54 GB (77.47 million rows/s., 1.31 GB/s.)
Peak memory usage: 1.38 GiB.
```

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
SELECT
    count(*),
    `Payment Type`,
    toMonth(trip_start_date) AS mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
WHERE mnth = 8
GROUP BY
    `Payment Type`,
    mnth
ORDER BY count(*) DESC
FORMAT `Null`

Query id: 8cafbd90-bf38-4dc5-83fd-041eaadab565

Ok.

0 rows in set. Elapsed: 1.127 sec. Processed 17.91 million rows, 301.77 MB (15.90 million rows/s., 267.85 MB/s.)
Peak memory usage: 285.48 MiB.
```

301MB vs 3.3GB from BQ

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
SELECT
    count(),
    `Payment Type`,
    toMonth(trip_start_date) AS mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
WHERE (trip_start_date >= '2021-01-01') AND (trip_start_date <= '2021-12-31')
GROUP BY
    `Payment Type`,
    mnth
ORDER BY count(*) DESC
FORMAT `Null`

Query id: 397d2026-3175-4a95-bc3d-f9ff78bb802e

Ok.

0 rows in set. Elapsed: 1.001 sec. Processed 3.95 million rows, 66.79 MB (3.94 million rows/s., 66.72 MB/s.)
Peak memory usage: 96.76 MiB.
```

About the same data is BigQuery

Overall it's amazing that this performance is had a over 4k data files! Now let's merge them all up and rerun queries :)

#### Merge performance

#### Queries (post merge, direct file access, m7a.32xlarge 128vCPU) 