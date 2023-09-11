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
it's sub-linear time to upload high partitions counts. The following snippet was from a 16 vCPU machine using 16 
max_threads:

```
flushed 100000 rows and 62 files in 2.939800262451172 seconds
flushed 100000 rows and 63 files in 3.041750907897949 seconds
flushed 100000 rows and 62 files in 2.985896587371826 seconds
flushed 100000 rows and 63 files in 2.866239070892334 seconds
flushed 100000 rows and 62 files in 3.1683757305145264 seconds
flushed 100000 rows and 61 files in 2.8781652450561523 seconds
flushed 100000 rows and 62 files in 2.921379804611206 seconds
flushed 100000 rows and 62 files in 3.026604175567627 seconds
flushed 100000 rows and 63 files in 2.9038808345794678 seconds
flushed 100000 rows and 668 files in 11.152297019958496 seconds
flushed 100000 rows and 579 files in 9.737096071243286 seconds
flushed 100000 rows and 575 files in 9.660438776016235 seconds
flushed 100000 rows and 618 files in 10.305824518203735 seconds
flushed 100000 rows and 456 files in 8.050954341888428 seconds
flushed 100000 rows and 316 files in 6.332479238510132 seconds
flushed 100000 rows and 293 files in 5.932677984237671 seconds
flushed 100000 rows and 297 files in 5.954922199249268 seconds
flushed 100000 rows and 302 files in 6.023008108139038 seconds
flushed 100000 rows and 320 files in 6.437815189361572 seconds
flushed 100000 rows and 340 files in 6.7036871910095215 seconds
flushed 100000 rows and 653 files in 10.860777616500854 seconds
flushed 100000 rows and 566 files in 9.697846174240112 seconds
flushed 100000 rows and 509 files in 8.910516500473022 seconds
flushed 100000 rows and 414 files in 7.625535011291504 seconds
flushed 100000 rows and 493 files in 8.731367349624634 seconds
flushed 100000 rows and 366 files in 7.656696796417236 seconds
flushed 100000 rows and 373 files in 7.180020809173584 seconds
flushed 100000 rows and 359 files in 6.955235719680786 seconds
flushed 100000 rows and 357 files in 6.889531135559082 seconds
flushed 100000 rows and 308 files in 6.294888019561768 seconds
flushed 100000 rows and 331 files in 6.617126941680908 seconds
flushed 100000 rows and 304 files in 6.184493541717529 seconds
flushed 100000 rows and 288 files in 5.968321800231934 seconds
flushed 100000 rows and 266 files in 5.710318088531494 seconds
```

it performs shockingly well at high partition counts. In reality inserts should never touch more than a few 
partitions. If doing <10 partitions and using a custom minio cluster, this size could easily push millions of 
inserts per second. Double up the core count to achieve that with S3.