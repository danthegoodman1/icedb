# IceDB

An in-process parquet merge engine for S3. Inserts, merges, and tombstone cleanup powered by Python and DuckDB. 
IceDB runs stateless with a log in S3, meaning that you only pay for storage and compute during operations, enabling 
true serverless analytical processing.

The IceDB log keeps track of alive data files, as well as the running schema which is updated via insertion. It does 
so in an open and easily readable format to allow for any language or framework to parse the icedb log and read the 
alive files. Query engines such as DuckDB, ClickHouse, CHDB, Datafusion, Pandas, or custom parquet readers in any 
  language can easily read IceDB data in hundreds milliseconds, 
especially when combined with the [IceDB S3 Proxy](https://github.com/danthegoodman1/IceDBS3Proxy). See more in the 
[ARCHITECTURE.md](ARCHITECTURE.md)

IceDB is targeted at data warehouse use cases to replace systems like BigQuery, Athena, and Snowflake, but with 
clever data design can also replace provisioned solutions such as a ClickHouse cluster, Redshift, and more.

It is also ideal for multi-tenant workloads where your end users want to directly submit SQL

<!-- TOC -->
* [IceDB](#icedb)
  * [Performance test](#performance-test)
  * [Examples](#examples)
  * [Comparisons](#comparisons)
    * [Why IceDB?](#why-icedb)
    * [Why not BigQuery or Athena?](#why-not-bigquery-or-athena)
    * [Why not ClickHouse, TimescaleDB, RedShift, etc?](#why-not-clickhouse-timescaledb-redshift-etc)
    * [Why not the Spark/Flink/EMR ecosystem](#why-not-the-sparkflinkemr-ecosystem)
    * [When not to use IceDB](#when-not-to-use-icedb)
  * [Usage](#usage)
    * [`partition_strategy`](#partitionstrategy)
    * [`sort_order`](#sortorder)
    * [`format_row`](#formatrow)
    * [`unique_row_key`](#uniquerowkey)
  * [Pre-installing DuckDB extensions](#pre-installing-duckdb-extensions)
  * [Merging](#merging)
  * [Concurrent merges](#concurrent-merges)
  * [Tombstone cleanuip](#tombstone-cleanuip)
  * [Custom Merge Query (ADVANCED USAGE)](#custom-merge-query-advanced-usage)
    * [Handling `_row_id`](#handling-rowid)
      * [Deduplicating Data on Merge](#deduplicating-data-on-merge)
      * [Replacing Data on Merge](#replacing-data-on-merge)
      * [Aggregating Data on Merge](#aggregating-data-on-merge)
<!-- TOC -->

## Performance test

From the test, inserting 2000 times with 2 parts, shows performance against S3 and reading in the state and schema

```
============== insert hundreds ==============
this will take a while...
inserted 200
inserted hundreds in 11.283345699310303
reading in the state
read hundreds in 0.6294591426849365
files 405 logs 202
verify expected results
got 405 alive files
[(406, 'a'), (203, 'b')] in 0.638556957244873
merging it
merged partition cust=test/d=2023-02-11 with 203 files in 1.7919442653656006
read post merge state in 0.5759727954864502
files 406 logs 203
verify expected results
got 203 alive files
[(406, 'a'), (203, 'b')] in 0.5450308322906494
merging many more times to verify
merged partition cust=test/d=2023-06-07 with 200 files in 2.138633966445923
merged partition cust=test/d=2023-06-07 with 3 files in 0.638775110244751
merged partition None with 0 files in 0.5988118648529053
merged partition None with 0 files in 0.6049611568450928
read post merge state in 0.6064021587371826
files 408 logs 205
verify expected results
got 2 alive files
[(406, 'a'), (203, 'b')] in 0.0173952579498291
tombstone clean it
tombstone cleaned 4 cleaned log files, 811 deleted log files, 1012 data files in 4.3332929611206055
read post tombstone clean state in 0.0069119930267333984
verify expected results
got 2 alive files
[(406, 'a'), (203, 'b')] in 0.015745878219604492

============== insert thousands ==============
this will take a while...
inserted 2000
inserted thousands in 107.14211988449097
reading in the state
read thousands in 7.370793104171753
files 4005 logs 2002
verify expected results
[(4006, 'a'), (2003, 'b')] in 6.49034309387207
merging it
breaking on marker count
merged 2000 in 16.016802072525024
read post merge state in 6.011193037033081
files 4006 logs 2003
verify expected results
[(4006, 'a'), (2003, 'b')] in 6.683710098266602
# laptop became unstable around here
```

Some notes:

1. Very impressive state read performance with so many files (remember it has to open each one and accumulate the 
   state!)
2. Merging happens very quick
3. Tombstone cleaning happens super quick as well
4. DuckDB performs surprisingly well with so many files (albeit they are one or two rows each)
5. At hundreds of log files and partitions (where most tables should live at), performance was exceptional
6. Going from hundreds to thousands, performance is nearly perfectly linear, sometimes even super linear (merges)!

Having such a large log files (merged but not tombstone cleaned) is very unrealistic. Chances are worst case you 
have <100 log files and hundreds or low thousands of data files. Otherwise you are either not merging/cleaning 
enough, or your partition scheme is far too granular.

The stability of my laptop struggled when doing the thousands test, so I only showed where I could consistently get to.

## Examples

See the [examples](examples) folder for many examples like Materialized Views, custom merge queries, schema 
validation before insert, and more.

- [Simple example](examples/simple.py)
- [Materialized View example](examples/materialized-view.py)
- Custom merge queries [aggregation](examples/custom-merge-aggregation.py) and [replacing](examples/custom-merge-replacing.py)
- [Verify schema before insert](examples/verify-schema.py)
- API using [flask](examples/api-flask.py) and [falcon](examples/api-falcon.py)
- [Segment webhook sink](examples/segment-webhook-sink.py)

## Comparisons

IceDB was made to fill the gap between solutions like ClickHouse and BigQuery.

### Why IceDB?

IceDB offers many novel features out of the box that comparable data warehouses and OLAP DBs don't:

- Native multi-tenancy with prefix control and the [IceDB S3 Proxy](https://github.com/danthegoodman1/IceDBS3Proxy), 
  including letting end-users write their own SQL queries
- True separation of data storage, metadata storage, and compute with shared storage (S3)
- Multiple options for query processing (DuckDB, ClickHouse, CHDB, Datafusion, Pandas, custom parquet readers in any 
  language)
- Open data formats for both the log and data storage
- Extreme flexibility in functionality due to being in-process and easily manipulated for features like materialized 
  views, AggregatingMergeTree and ReplacingMergeTree-like functionality

### Why not BigQuery or Athena?

BigQuery offers a great model of only paying for S3-price storage when not querying, and being able to summon 
massive resources to fulfill queries when requested. The issues with BigQuery (and similar like Athena) is that:
- They are overly expensive at $5/TB processed
- They are limited to their respective cloud providers
- Closed source, no way to self-host or contribute
- Only one available query engine

For example, queries on data that might cost $6 on BigQuery would only be around ~$0.10 running IceDB and 
dynamically provisioning a CLickHouse cluster on fly.io to respond to queries. That's a cost reduction of 60x 
without sacrificing performance.

While IceDB does require that you manage some logic like merging and tombstone cleaning yourself, the savings, 
flexibility, and performance far outweigh the small management overhead.

To get the best performance in this model, combine with the
[IceDB S3 Proxy](https://github.com/danthegoodman1/IceDBS3Proxy)

### Why not ClickHouse, TimescaleDB, RedShift, etc?

We love ClickHouse, in fact it's our go-to query engine for IceDB in the form of
[BigHouse](https://github.com/danthegoodman1/bighouse) (dynamically provisioned ClickHouse clusters)

The issue with these solutions are the tight coupling between storage, metadata, and compute. The lack of elasticity 
in these systems require that
have the resources to answer massive queries idle and ready, while also requiring massive resources for inserting 
when large queries are only occasional (but need to be answered quickly).

IceDB allows for ingestion, merging, tombstone cleaning, and querying all in a serverless model due to compute being 
effectively stateless, with all state being managed on S3 (plus some coordination if needed).

Ingestion workers can be summoned per-request, merging and tombstone cleaning can be on timers, and querying can 
provision resources dynamically based on how much data needs to be read.

### Why not the Spark/Flink/EMR ecosystem

Beyond the comical management overhead, performance is shown to be inferior to other solutions, and the flexibility 
of these systems is paid 10-fold over in complexity.

### When not to use IceDB

- If you need answers in <100ms, consider ClickHouse or Tinybird (well-designed materialized views in IceDB can provide 
  this 
  performance level as well)
- If you need tight integrations with cloud-provider-specific integrations and can't spare writing a little extra 
  code, consider BigQuery and Athena
- If your network cards are not very fast, consider ClickHouse
- If you can't access an S3 service from within the same region/datacenter, and are not able to host something like 
  minio yourself, consider ClickHouse
- If you need something 100% fully managed, depending on your needs and budget consider managed ClickHouse 
  (Altinity, DoubleCloud, ClickHouse Cloud, Aiven), Tinybird, BigQuery, or Athena

## Tips before you dive in

### Insert in large batches

Performance degrades linearly with more files because the log gets larger, and the number of parquet files in S3 to 
be read (or even just listed) grows. Optimal performance is obtained by inserting as infrequently as possible. For 
example, you might write records to RedPanda first, and have workers that insert in large batches every 3 seconds. 
Or maybe you buffer in memory first from your API nodes, and flush that batch to disk every 3 seconds
([example](examples/api-flask.py))

### Merge and Tombstone clean often

Merging increases the performance of queries by reducing the number of files that data is spread across. Merging 
combines files within the same partition. Parquet natively providers efficient indexing to ensure that selective 
queries remain performant, even if you are only selecting thousands of rows out of millions from a single parquet file.

Tombstone cleaning removes dead data and log files from S3, and also removes the tracked tombstones in the active 
log files. This improves the performance of queries by making the log faster to read, but does not impact how 
quickly the query engine can read the data once it knows the list of parquet files to read.

The more frequently you insert batches, the more frequently you need to merge. And the more frequently you merge, 
the more frequently you need to clean tombstones in the log and data files.

### Large partitions, sort your data well!

Do not make too many partitions, they do very little to improve query performance, but too many partitions will 
greatly impair query performance.

The best thing you can do to improve query performance is to sort your data in the same way you'd query it, and 
write it to multiple tables if you need multiple access patterns to your data.

For example, if you ingest events from a webapp like Mixpanel and need to be able to query events by a given user over 
time, and a 
single event over time, then you should create two tables:

- A table with a partition format `uid={user_id}` and a sort of `timestamp,event_id` for listing a user's events 
  over time (user-view)
- A table with a partition format of `d={YYYY-MM-DD}` and a sort of `event_id,timestamp` for finding events over time 
  (dashboards and insights)

## Usage

```
pip install git+https://github.com/danthegoodman1/icedb
```

```python
from icedb import IceDBv3

ice = IceDBv3(...)
```

### `partition_strategy`

Function that takes in an `dict`, and returns a `str`. How the partition is determined from the row dict.

Example:

```python
from datetime import datetime
from icedb import IceDBv3

def part_strat(row: dict) -> str:
    rowTime = datetime.utcfromtimestamp(row['timestamp']/1000) # timestamp is in ms
    return 'y={}/m={}/d={}'.format('{}'.format(rowTime.year).zfill(4), '{}'.format(rowTime.month).zfill(2), '{}'.format(rowTime.day).zfill(2))

ice = IceDBv3(partition_strategy=part_strat, sort_order=['event', 'timestamp'])
```

### `sort_order`

Defines the order of top-level keys in the row dict that will be used for sorting inside of the parquet file.

Example:

```python
['event', 'timestamp']
```

### `format_row`

Format row is the function that will determine how a row is finally formatted to be inserted. This is where you would want to flatten JSON so that the data is not corrupted. This is called just before inserting into a parquet file.

**It's crucial that you flatten the row and do not have nested objects**

Example:

```python
import json

def format_row(row: dict) -> dict:
    row['properties'] = json.dumps(row['properties']) # convert nested dict to json string
    return row
```

For something like a Materialized View that keeps a running count, we need to seed the row with an initial count, so 
we can sum that count in the custom merge query (and data queries) later. See [this example](examples/custom-merge-aggregation.py).

```python
import json

def format_row(row: dict) -> dict:
    row['properties'] = json.dumps(row['properties']) # convert nested dict to json string
    row['cnt'] = 1 # seed the count
    return row
```

### `unique_row_key`

If provided, will use a top-level row key as the `_row_id` for deduplication. If not provided a UUID will be generated.

## Pre-installing DuckDB extensions

DuckDB uses the `httpfs` extension. See how to pre-install it into your runtime here: https://duckdb.org/docs/extensions/overview.html#downloading-extensions-directly-from-s3

and see the `extension_directory` setting: https://duckdb.org/docs/sql/configuration.html#:~:text=PHYSICAL_ONLY-,extension_directory,-Set%20the%20directory with the default of `$HOME/.duckdb/`

You may see an example of this in the [example Dockerfile](Dockerfile.example).

## Merging

Merging takes a `max_file_size`. This is the max file size that is considered for merging, as well as a threshold for when merging will start. This means that the actual final merged file size (by logic) is in theory 2*max_file_size, however due to parquet compression it hardly ever gets that close.

For example if a max size is 10MB, and during a merge we have a 9MB file, then come across another 9MB file, then 
the threshold of 10MB is exceeded (18MB total) and those files will be merged. However, with compression that final file might be only 12MB in size.

## Concurrent merges

Concurrent merges won't break anything due to the isolation level employed in the meta store transactions, however there is a chance that competing merges can result in conflicts, and when one is detected the conflicting merge will exit. Instead, you can choose to immediately call `merge` again (or with a short, like 5 seconds) if you successfully merged files to ensure that lock contention stays low.

However, concurrent merges in opposite directions is highly suggested.

For example in the use case where a partition might look like `y=YYYY/m=MM/d=DD` then you should merge in `DESC` 
order frequently (say once every 15 seconds). This will keep the hot partitions more optimized so that queries on current data don't get too slow. These should have smaller file count and size requirements, so they can be fast, and reduce the lock time of files in the meta store.

You should run a second, slower merge internal in `ASC` order that fully optimizes older partitions. These merges can be much large in file size and count, as they are less likely to conflict with active queries. Say this is run every 5 or 10 minutes.

## Tombstone cleanup

Using the `remove_inactive_parts` method, you can delete files with some minimum age that are no longer active. This helps keep S3 storage down.

For example, you might run this every 10 minutes to delete files that were marked inactive at least 2 hours ago.

## Custom Merge Query (ADVANCED USAGE)

You can optionally provide a custom merge query to achieve functionality such as aggregate-on-merge or replace-on-merge as found in the variety of ClickHouse engine tables such as the AggregatingMergeTree and ReplacingMergeTree.

This can also be used along side double-writing (to different partition prefixes) to create materialized views!

**WARNING: If you do not retain your merged files, bugs in merges can permanently corrupt data. Only customize merges if you know exactly what you are doing!**

This is achieved through the `custom_merge_query` function. You should not provide any parameters to this query.

The default query is:

```sql
select *
from source_files
```

The `?` **must be included**, and is the list of files being merged.

`source_files` is just an alias for `read_parquet(?, hive_partitioning=1)`, which will be string-replaced if it exists. Note that the `hive_partitioning` columns are virtual, and do not appear in the merged parquet file, therefore is it not needed.

### Handling `_row_id`

#### Deduplicating Data on Merge

By default, no action is taken on `_row_id`. However you can use this to deduplicate in both analytical queries, and custom merge queries.

For example, if you wanted merges to take any (but only a single) value for a given `_row_id`, you might use:

```sql
select
    any_value(user_id),
    any_value(properties),
    any_value(timestamp),
    _row_id
from source_files
group by _row_id
```

Note that this will only deduplicate for a single merged parquet file, to guarantee single rows you much still employ deduplication in your analytical queries.

#### Replacing Data on Merge

If you wanted to replace rows with the most recent version, you could write a custom merge query that looks like:

```sql
select
    argMax(user_id, timestamp),
    argMax(properties, timestamp),
    max(timestamp),
    _row_id
from source_files
group by _row_id
```

Like deduplication, you must handle this in your queries too if you want to guarantee getting the single latest row.

#### Aggregating Data on Merge

If you are aggregating, you must include a new `_row_id`. If you are replacing this should come through choosing the correct row to replace.

Example aggregation merge query:

```sql
select
    user_id,
    sum(clicks) as clicks,
    gen_random_uuid()::TEXT as _row_id
from source_files
group by user_id
```

This data set will reduce the number of rows over time by aggregating them by `user_id`.

**Pro-Tip: Handling Counts**

Counting is a bit trickier because you would normally have to pivot from `count()` when merging a never-before-merged file to `sum()` with files that have been merged at least once to account for the new number. The trick to this is instead adding a `counter` column with value `1` every time you insert a new row.

Then, when merging, you simply `sum(counter) as counter` to keep a count of the number of rows that match a condition.