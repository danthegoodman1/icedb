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
flushed 1000000 rows and 21 files in 17.91532826423645 seconds
flushed 1000000 rows and 12 files in 17.58492422103882 seconds
flushed 1000000 rows and 12 files in 17.568284511566162 seconds
flushed 1000000 rows and 12 files in 18.191020488739014 seconds
flushed 1000000 rows and 13 files in 17.931345462799072 seconds
flushed 1000000 rows and 14 files in 18.225353002548218 seconds
flushed 1000000 rows and 14 files in 18.443052530288696 seconds
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