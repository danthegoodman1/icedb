# IceDB v3 Architecture

## Log file(s)

IceDB keeps track of the active files and schema in a log, much like other database systems. This log is stored in S3, and is append-only. This log can also be truncated via a tombstone cleanup process described below.

Both the schema and active files are tracked within the same log file, and in each log file.

### Log file structure

The log file is new-line delimited JSON, with the line being special. The first line is in the schema (typescript format):

```ts
interface {
  v: string // the version number
  sch: number // line number that the accumulated schema begins at
  tmb?: number // line number that the list of log file tombstone start at
  f: number // line number that the list of file markers begins at
}
```

#### Schema (sch)

There is only one schema line per file, taking the form:

```ts
interface {
  [column: string]: string // example: "user_id": "VARCHAR"
}
```

Columns are never removed from the schema, and they always consist of the union of log file schemas.

If data type conflicts are found (e.g. log file A has a column as a VARCHAR, but log file B has a column as a BIGINT), then the merge fails and error logs are thrown. This can be mitigated by having the ingestion instances read the schema periodically and caching in memory (only need to read log files up through the last schema line, then can abort request). One could also choose to use a transactionally-secure schema catalog to protect this, have data sources declare their schema ahead of time, and more to validate the schema.

#### Log file tombstones (tmb)

These are the files that were merged and updated into this file. If log files A and B were merged into C, not all part files listed in A and B were necessarily merged into new parts marked in C. Because of this, files that existed in A and B that were not part of the merge are copied to log file C in the alive status. Any files that were merged are marked as not alive.

Because log files A and B were merged into C, we created "tombstones" for log files A and B. Tombstones are kept track of so that some background cleaning process can remove the merged log files after some grace period (for example files older than the max query timeout possible * 2). This is why it's important to insert infrequently and in large batches.

They take the format:

```ts
interface {
  "p": string // the file path, e.g. /some/prefixed/_log/ksuid.jsonl
  "t": number // the timestamp when this log file was first merged
}
```

#### File marker (f)

There are at least one file lines per log file, taking the form:

```ts
interface {
  "p": string // the file path, e.g. /some/prefixed/file.parquet
  "b": number // the size in bytes
  "t": number // created timestamp in milliseconds
  "tmb": string // the path of the log file this was merged from, indicating that this file is not alive. When tombstone cleanup deletes this log file, it will also delete this file marker
}
```

## Merging

Merging required coordination with an exclusive lock.

When a merge occurs, both data parts and log files are merged. A newly created log file is the combination of:

1. New files created in the merge (should be 1) (`f`)
2. Files that were part of the merged, marked as not alive (with their tombstone tracked) (`f`)
3. Files that were not part of the merge, marked alive (`f`)
4. Log files that contained alive data files that were part of the merge (`src`)

The reason for copying the state of untouched files is that the new log file represents a new view of modified data. If log files A and B were merged into C, then A and B represent a stale version of the data and only exist to prevent breaking existing listquery operations from not being able to find their files.

Merged log files are not deleted immediately to prevent issues with currently running operations, and are marked as merged in the new log file so they are known to be able to be cleaned up after some interval. You want to ensure that files are only cleaned well after they could be in use (say multiple times the max e2e query timeout, including list operation times). This is why it's important to insert infrequently and in large batches, to prevent too many files from building up before they can be deleted.

Data part files that were part of the data merge are marked as alive so in the event that a list operation sees files A, B, and C, it knows that the old files were merged. If it only ends up seeing A and B, then it just gets a stale view of the data. This is why it's important to ensure that a single query gets a constant-time view of the database, so nested queries do not cause an inconsistent view of the data.

Tomestones include the timestamp when they were first merged for the tombstone cleanup worker. When files merge, the must always carry forward any found tombstone. Tombstone cleanup is idempotent so in the event that a merge occurs concurrently with tombstone cleanup there is no risk of data loss or duplication. Merging must also always carry forward file markers that have tombstones, as these are also removed by the tomestone cleanup process.

## Tombstone cleanup

The second level of coordination with a second exclusive lock that is needed is tombstone cleaning. There is a separate `gc_grace_seconds` parameter that controls how long tombstone files are kept for.

When tombstone cleanup occurs, the entire state of the log is read. Any tombstones that are found older than the `gc_grace_seconds` are deleted from S3.

When the cleaning process finds a log file with tombstones, it first deletes those files from S3. If that is successful (not found errors being idempotent passes), then the log files is replaced with the same contents, minus the tombstones.

Tombstone cleanup is also responsible for clearing out file markers with a linked tombstone.

TLDR: The tombstone process deletes files in S3 that have tombstones, and rewrites the log files without the tombstones and the file markers that are linked to those tombstones.
