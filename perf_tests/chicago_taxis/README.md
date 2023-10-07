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

0B vs 1.56GB on BigQuery... We did do S3 listing calls and extracted the month from the partition so it wasn't 
totally fair, but the cost was still orders of magnitude less than BigQuery.

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
more granular row groups would perform a lot better.

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

```
Merged partition d=2018-05 with 32 files in 14.967223167419434 seconds
Merged partition d=2018-05 with 3 files in 15.671793460845947 seconds
Merged partition d=2017-10 with 32 files in 15.577917337417603 seconds
Merged partition d=2017-10 with 2 files in 14.891311407089233 seconds
Merged partition d=2018-03 with 32 files in 16.53251338005066 seconds
Merged partition d=2018-03 with 2 files in 14.830085754394531 seconds
Merged partition d=2017-12 with 31 files in 16.494601249694824 seconds
Merged partition d=2017-12 with 2 files in 14.961239576339722 seconds
Merged partition d=2017-02 with 30 files in 15.911055326461792 seconds
Merged partition d=2017-02 with 3 files in 15.94778323173523 seconds
done in 4712.57857465744 seconds
```

Merge performance here seems to be entirely based on log size, as there is no difference between merging 30 small files 
and 3 larger files. Running an empty merge takes 16.21 seconds :P. This indicates to me that the time to actually 
process files for merging even 40 files was well beyond sub-second.

Files were kept at a max size ~100MB (which was too small, and hampered performance due to high time ot first byte 
latency in S3).

#### Queries (post merge and tombstone clean (2x), direct file access, m7a.32xlarge 128vCPU)

Tombstone cleaning was run twice to make sure that all the old log files were cleaned up as well. This is fully 
optimized, which is a more fair comparison to BigQuery as that dataset will have been fully merged and optimized as 
well.

```
SELECT
    count(),
    uniq(_file)
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')

Query id: 975cbf0b-0140-42ba-b801-ff708ffd1593

┌───count()─┬─uniq(_file)─┐
│ 209512921 │         184 │
└───────────┴─────────────┘
```

Much smaller file count, there are a few partitions with a handful of files, but many of them are only 1.

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS mnth
SELECT
    count() AS cnt,
    mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY mnth
ORDER BY mnth DESC
FORMAT `Null`

Query id: c3656856-5e80-48e3-ba0d-5c116ea7f8b9

Ok.

0 rows in set. Elapsed: 0.807 sec. Processed 209.51 million rows, 0.00 B (259.71 million rows/s., 0.00 B/s.)
Peak memory usage: 441.70 MiB.
```

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
    FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
    GROUP BY mnth
)
ORDER BY mnth DESC
FORMAT `Null`

Query id: 26a22369-8dea-4b49-92c8-b2b8dcbabbde

Ok.

0 rows in set. Elapsed: 5.778 sec. Processed 209.51 million rows, 11.36 GB (36.26 million rows/s., 1.97 GB/s.)
Peak memory usage: 502.03 MiB.
```

This actually slowed down a bit, which again I believe is attributed to the relatively large row groups inside the 
parquet files.

```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`

Query id: 1659a3c1-429f-4aa5-a07e-055d402cc188

Ok.

0 rows in set. Elapsed: 3.054 sec. Processed 209.51 million rows, 3.54 GB (68.59 million rows/s., 1.16 GB/s.)
Peak memory usage: 197.01 MiB.
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

Query id: 75fae04e-1bd7-438c-b6c0-2864771e0529

Ok.

0 rows in set. Elapsed: 1.176 sec. Processed 17.91 million rows, 301.77 MB (15.23 million rows/s., 256.61 MB/s.)
Peak memory usage: 20.56 MiB.
```

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

Query id: b00add9d-3d39-4f8d-9f42-635dfd836afb

Ok.

0 rows in set. Elapsed: 0.680 sec. Processed 3.95 million rows, 66.79 MB (5.81 million rows/s., 98.23 MB/s.)
Peak memory usage: 17.35 MiB.
```

As we can see the queries that benefit from the partitioning have the largest speed difference. But otherwise 
queries remain about the same performance due to a large row group size in the parquet files. In fact, due to this 
large size more files is actually a bit faster to go through because there can be slightly more concurrency!

#### Queries (post merge and tombstone clean, 8192 row group size, direct file access, m7a.32xlarge 128vCPU)

Next lets look what happens if we use smaller row groups (similar to clickhouse index granularity)

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS mnth
SELECT
    count() AS cnt,
    mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY mnth
ORDER BY mnth DESC
FORMAT `Null`

Query id: 10256598-69ce-4c4f-9ff8-9601a6d0d6c4

Ok.

0 rows in set. Elapsed: 0.205 sec. Processed 209.51 million rows, 0.00 B (1.02 billion rows/s., 0.00 B/s.)
Peak memory usage: 946.42 KiB.
```

Now we're talking, that was with 16 cores... lets bump it to 128

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS mnth
SELECT
    count() AS cnt,
    mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m_8k/_data/**/*.parquet')
GROUP BY mnth
ORDER BY mnth DESC
FORMAT `Null`

Query id: 2a97d5ca-f5af-4bad-92bb-69070f73ffad

Ok.

0 rows in set. Elapsed: 0.162 sec. Processed 209.51 million rows, 0.00 B (1.29 billion rows/s., 0.00 B/s.)
Peak memory usage: 4.69 MiB.
```

Kachow

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
    FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m_8k/_data/**/*.parquet')
    GROUP BY mnth
)
ORDER BY mnth DESC
FORMAT `Null`

Query id: 4bca752c-03a7-4e57-a5b7-1dadf0fa289d

Ok.

0 rows in set. Elapsed: 5.874 sec. Processed 209.51 million rows, 11.36 GB (35.67 million rows/s., 1.93 GB/s.)
Peak memory usage: 478.98 MiB.
```

```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m_8k/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`

Query id: 558b8b39-ec74-429f-83bc-8806c5b37dc0

Ok.

0 rows in set. Elapsed: 2.866 sec. Processed 209.51 million rows, 3.54 GB (73.10 million rows/s., 1.24 GB/s.)
Peak memory usage: 194.03 MiB.
```

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
SELECT
    count(*),
    `Payment Type`,
    toMonth(trip_start_date) AS mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m_8k/_data/**/*.parquet')
WHERE mnth = 8
GROUP BY
    `Payment Type`,
    mnth
ORDER BY count(*) DESC
FORMAT `Null`

Query id: 04513d20-5f55-4620-ac45-7ebc33ebe21e

Ok.

0 rows in set. Elapsed: 1.765 sec. Processed 17.91 million rows, 301.77 MB (10.15 million rows/s., 171.02 MB/s.)
Peak memory usage: 20.36 MiB.
```

```
WITH parseDateTime(CAST(extract(_path, 'd=([^\\/]+)'), 'String'), '%Y-%m') AS trip_start_date
SELECT
    count(),
    `Payment Type`,
    toMonth(trip_start_date) AS mnth
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m_8k/_data/**/*.parquet')
WHERE (trip_start_date >= '2021-01-01') AND (trip_start_date <= '2021-12-31')
GROUP BY
    `Payment Type`,
    mnth
ORDER BY count(*) DESC
FORMAT `Null`

Query id: a0034894-1fde-4927-a729-e00d7560812a

Ok.

0 rows in set. Elapsed: 0.764 sec. Processed 3.95 million rows, 66.79 MB (5.17 million rows/s., 87.40 MB/s.)
Peak memory usage: 17.35 MiB.
```

##### Bonus m7a.8xlarge

```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`

Query id: 6988363a-98b5-4b1a-8941-e03f19c07fa3

Ok.

0 rows in set. Elapsed: 2.364 sec. Processed 209.51 million rows, 3.54 GB (88.63 million rows/s., 1.50 GB/s.)
Peak memory usage: 316.68 MiB.
```


But if I do it with 128 threads I get

```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`
SETTINGS max_threads = 128

Query id: 9508f8dc-fedc-4638-9e39-d169bc046b74

Ok.

0 rows in set. Elapsed: 1.197 sec. Processed 209.51 million rows, 3.54 GB (175.05 million rows/s., 2.96 GB/s.)
Peak memory usage: 199.05 MiB.
```


Which are all hot runs. Cold run was a LOT slower

```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`

Query id: 1cd0bc1b-b94c-42c8-b0f9-1affc133d2a6

Ok.

0 rows in set. Elapsed: 8.600 sec. Processed 209.51 million rows, 3.54 GB (24.36 million rows/s., 411.96 MB/s.)
Peak memory usage: 321.15 MiB.
```

##### Bonus, r6in.32xlarge

Cold run of r6in.32xlarge
```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`

Query id: 9b7c1985-232e-4e9e-a13a-58ca95796e34

Ok.

0 rows in set. Elapsed: 4.126 sec. Processed 209.51 million rows, 3.54 GB (50.78 million rows/s., 858.80 MB/s.)
Peak memory usage: 453.93 MiB.
```


Hot run:
```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`
SETTINGS max_threads = 128

Query id: 241ba99a-97ca-4ca9-8c68-8fd5b1dd28ed

Ok.

0 rows in set. Elapsed: 1.121 sec. Processed 209.51 million rows, 3.54 GB (186.89 million rows/s., 3.16 GB/s.)
Peak memory usage: 196.08 MiB.
```

Hot with more threads:

```
SELECT
    count(),
    `Payment Type`
FROM s3('https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/chicago_taxis_1m/_data/**/*.parquet')
GROUP BY `Payment Type`
ORDER BY count() DESC
FORMAT `Null`
SETTINGS max_threads = 300

Query id: 3899b91b-fc87-4007-9248-2a783a8d0a95

Ok.

0 rows in set. Elapsed: 1.261 sec. Processed 209.51 million rows, 3.54 GB (166.20 million rows/s., 2.81 GB/s.)
Peak memory usage: 203.01 MiB.
```

As we can see, once we start matching the threads to the number of files (186) we aren't getting much more benefit 
from relatively small single files (~60-200MB). However, even hot queries could range by upwards of 3x the above 
performance due to what must be random high latency file downloads. 

### Conclusion

We can see for IceDB, querying the parquet files from S3 with ClickHouse using a VPC endpoint and node with 100gbit 
nic, that S3 is the bottleneck. Not only did changing the file counts not help significantly (when it should), but 
the difference between a 32 core machine with 12.5 gbit nic and a 128 core machine with a 170gbit nic is zero. There 
was a max concurrency reached at around 1 thread/file that had the greatest performance, most likely because 
ClickHouse did not deem the files large enough to download parts concurrently. 166M rows/s is nothing to scoff at.

We compared hot performance to avoid expensive listing API calls, as with the IceDB S3 proxy these are much faster. 
For the 186 files, maybe 150 list calls were required due to the 

Use custom minio clusters on local nvmes with 100gbit nics if you want IceDB + ClickHouse to beat out BigQuery, 
especially if you use BigHouse (run clickhouse like bigquery)! With minio, you should expect to see multiple times 
speed boosts on query performance, but it's a lot more to manage and more expensive than S3 of course. As 
illustrated in 
https://altinity.com/blog/clickhouse-object-storage-performance-minio-vs-aws-s3 we can see that querying with 16 
cores to minio is faster than 64 to S3 💀. I would expect a sufficient minio cluster to outperform S3 by 3-5x in 
terms of reducing query response times, and also have far less variance in response times

**Ultimately comparing queries on S3 + ClickHouse using IceDB vs BigQuery, BigQuery loses in price/performance by a lot.**

For example even just the same data set size, S3 doesn't charge egress in the same region, so a query that executes 
a bit slower on ClickHouse using a 128vCPU machine costs less than a cent, where a comparable BigQuery one might take 
one 
second but read 77GB, costing ~$0.54. That's >54x more expensive to run a query on BigQuery. But then again I bet 
ClickHouse with 1/4 the resources queries 2x as fast as BigQuery with minio.

For these queries, BigQuery used 7min 56sec slot time, which is ~4076 vCPU.

### Bonus: Testing the IceDB S3 Proxy

I didn't use the proxy in this test because in all testing, I found the S3 proxy to perform as well as S3 directly:

```
root@ip-10-0-166-178:~# curl http://localhost:8080/icedb-test-tangia-staging/wav2lip_gan.pth --output out
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  415M    0  415M    0     0  95.2M      0 --:--:--  0:00:04 --:--:-- 95.3M
root@ip-10-0-166-178:~# curl https://s3.us-east-1.amazonaws.com/icedb-test-tangia-staging/namespace/user_abc/wav2lip_gan.pth --output out
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  415M  100  415M    0     0  94.5M      0  0:00:04  0:00:04 --:--:-- 94.5M
```