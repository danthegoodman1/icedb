# IceDB v3 Architecture

## Log file(s)

IceDB keeps track of the active files and schema in a log, much like other database systems. This log is stored in S3, and is append-only. This log can also be truncated when the merge lock is acquired.

Both the schema and active files are tracked within the same log file, and in each log file.

The contents of the log are sorted by time (time the part became active). It represents a sequence of operations such as ADD/DEL to determine when files become active and inactive.

### Log file structure

The log file is new-line delimited JSON, with the line being special. The first line is in the schema (typescript format):

```ts
interface {
  v: string // the version number
  sch: number // line number that the accumulated schema begins at
  tmb?: number // line number that the list of log file tombstone start at
  f: number // line number that the list of files begins at
}
```

#### Schema (sch)

There is only one schema line per file, taking the form:

```ts
interface {
  [column: string]: string // example: "user_id": "VARCHAR"
}
```

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

#### File (f)

There are at least one file lines per log file, taking the form:

```ts
interface {
  "p": string // the file path, e.g. /some/prefixed/file.parquet
  "b": number // the size in bytes
  "t": number // created timestamp in milliseconds
  "a": bool // whether the file is active
}
```

## Merging

Merging required coordination with an exclusive lock.

When a merge occurs, both data parts and log files are merged. A newly created log file is the combination of:

1. New files created in the merge (should be 1) (`f`)
2. Files that were part of the merged, marked as not alive (`f`)
3. Files that were not part of the merge, marked alive (`f`)
4. Log files that contained alive data files that were part of the merge (`src`)

The reason for copying the state of untouched files is that the new log file represents a new view of modified data. If log files A and B were merged into C, then A and B represent a stale version of the data and only exist to prevent breaking existing listquery operations from not being able to find their files.

Merged log files are not deleted immediately to prevent issues with currently running operations, and are marked as merged in the new log file so they are known to be able to be cleaned up after some interval. You want to ensure that files are only cleaned well after they could be in use (say multiple times the max e2e query timeout, including list operation times). This is why it's important to insert infrequently and in large batches, to prevent too many files from building up before they can be deleted.

Data part files that were part of the data merge are marked as alive so in the event that a list operation sees files A, B, and C, it knows that the old files were merged. If it only ends up seeing A and B, then it just gets a stale view of the data. This is why it's important to ensure that a single query gets a constant-time view of the database, so nested queries do not cause an inconsistent view of the data.

Merged log file markers also store the timestamp when they were first merged, this is so that those markers can be safely dropped during merge when the timestamp is older than 2x the interval at which the log files would be deleted by the background cleanup process.

## Tombstone cleanup

The second level of coordination with a second exclusive lock that is needed is tombstone cleaning. There is a separate `gc_grace_seconds` parameter that controls how long tombstone files are kept for.

When tombstone cleanup occurs, the entire state of the log is read. Any tombstones that are found older than the `gc_grace_seconds` are deleted from S3.

WIP: How do we handle removing the tombstones from the log file without rewriting it and creating more tombstones???