import os
from typing import List, Callable, Dict
import duckdb
from uuid import uuid4
from .log import (IceLogIO, Schema, LogMetadata, S3Client, FileMarker, LogTombstone, get_log_file_info,
                 LogMetadataFromJSON, LogTombstoneFromJSON, FileMarkerFromJSON)
from time import time, sleep
import json
from enum import Enum
import pyarrow as pa
from copy import deepcopy
import concurrent.futures


class CompressionCodec(Enum):
    UNCOMPRESSED = "UNCOMPRESSED"
    SNAPPY = "SNAPPY"
    ZSTD = "ZSTD"
    GZIP = "GZIP"


PartitionFunctionType = Callable[[dict], str]
PartitionRemovalFunctionType = Callable[[list[str]], list[str]]

class IceDBv3:
    partition_function: PartitionFunctionType
    sort_order: List[str]
    data_s3c: S3Client
    log_s3c: S3Client
    custom_merge_query: str | None
    custom_insert_query: str | None
    row_group_size: int
    path_safe_hostname: str
    compression_codec: CompressionCodec
    preserve_partition: bool
    max_threads: int
    duckdb_ext_dir: str

    def __init__(
            self,
            partition_function: PartitionFunctionType,
            sort_order: List[str],
            s3_region: str,
            s3_access_key: str,
            s3_secret_key: str,
            s3_endpoint: str,
            s3_client: S3Client,
            path_safe_hostname: str,
            s3_use_path: bool = False,
            duckdb_ext_dir: str = None,
            custom_merge_query: str = None,
            custom_insert_query: str = None,
            row_group_size: int = 122_880,
            compression_codec: CompressionCodec = CompressionCodec.SNAPPY,
            preserve_partition: bool = False,
            max_threads: int = os.cpu_count(),
            log_s3_client: S3Client = None
    ):
        self.partition_function = partition_function
        self.sort_order = sort_order
        self.row_group_size = row_group_size
        self.path_safe_hostname = path_safe_hostname
        self.data_s3c = s3_client
        self.custom_merge_query = custom_merge_query
        self.custom_insert_query = custom_insert_query
        self.preserve_partition = preserve_partition
        self.max_threads = max_threads
        self.duckdb_ext_dir = duckdb_ext_dir
        self.s3_region = s3_region
        self.s3_access_key = s3_access_key
        self.s3_secret_key = s3_secret_key
        self.s3_endpoint = s3_endpoint
        self.s3_use_path = s3_use_path

        if log_s3_client is not None:
            self.log_s3c = log_s3_client
        else:
            self.log_s3c = s3_client

        if not isinstance(compression_codec, CompressionCodec):
            raise AttributeError(f"invalid compression codec '{compression_codec}', must be one of type CompressionCodec")

        self.compression_codec = compression_codec

    def get_duckdb(self) -> duckdb:
        """
        threadsafe creation of a duckdb session
        """
        ddb = duckdb.connect(":memory:")
        ddb.execute("install httpfs")
        ddb.execute("load httpfs")
        ddb.execute(f"SET s3_region='{self.s3_region}'")
        ddb.execute(f"SET s3_access_key_id='{self.s3_access_key}'")
        ddb.execute(f"SET s3_secret_access_key='{self.s3_secret_key}'")
        ddb.execute(f"SET s3_endpoint='{self.s3_endpoint.split('://')[1]}'")
        ddb.execute(f"SET s3_use_ssl={'false' if 'http://' in self.s3_endpoint else 'true'}")
        if self.s3_use_path:
            ddb.execute("SET s3_url_style='path'")
        if self.duckdb_ext_dir is not None:
            ddb.execute(f"SET extension_directory='{self.duckdb_ext_dir}'")
        return ddb

    def __get_file_partition(self, full_path: str) -> str:
        base_path = full_path.split("_data/")[1]
        path_parts = base_path.split("/")
        # remove the file name
        partition = '/'.join(path_parts[:-1])
        return partition

    def get_schema(self, rows: list[dict]) -> Schema:
        running_schema = Schema()

        # py arrow table
        _rows = pa.Table.from_pylist(rows)

        # get schema
        ddb = self.get_duckdb()
        ddb.execute("describe {}".format("select * from _rows" if self.custom_insert_query is None
                                                         else self.custom_insert_query))
        schema_arrow = ddb.arrow()
        running_schema.accumulate(list(map(lambda x: str(x), schema_arrow.column('column_name'))), list(map(lambda x:
         str(x), schema_arrow.column('column_type'))))
        return running_schema

    def __insert_part(self, part: str, part_ref: list[dict]) -> tuple[FileMarker, Schema]:
        running_schema = Schema()

        # upload parquet file
        filename = str(uuid4()) + '.parquet'
        path_parts = ['_data', part, filename]
        if self.data_s3c.s3prefix is not None:
            path_parts = [self.data_s3c.s3prefix] + path_parts
        fullpath = '/'.join(path_parts)

        # py arrow table for inserting into duckdb
        _rows = pa.Table.from_pylist(part_ref)

        # get schema
        ddb = self.get_duckdb()
        ddb.execute("describe select * from _rows")
        schema_arrow = ddb.arrow()
        running_schema.accumulate(list(map(lambda x: str(x), schema_arrow.column('column_name'))),
                                  list(map(lambda x: str(x), schema_arrow.column('column_type'))))

        # copy to parquet file
        retries = 0
        while retries < 3:
            try:
                # if retries > 0:
                    # print(f"retrying duckdb s3 upload try {retries}")
                ddb.execute("""
                            copy ({}) to 's3://{}/{}' (format parquet, codec '{}', row_group_size {})
                            """.format(
                    'select * from _rows order by {}'.format(
                        ','.join(self.sort_order)) if self.custom_insert_query is None else self.custom_insert_query,
                    self.data_s3c.s3bucket,
                    fullpath,
                    self.compression_codec.value,
                    self.row_group_size
                ))
                break
            except duckdb.HTTPException as e:
                if e.status_code < 500 and e.status_code != 429:
                    raise e
                if retries >= 3:
                    raise e
                retries += 1
                print(f"HTTP exception (code {e.status_code}) uploading part on try {retries}, sleeping "
                      f"{300*retries}ms before retrying")
                sleep(0.3*retries)
            except Exception as e:
                raise e

        insert_time = round(time() * 1000)

        # get file metadata
        obj = self.data_s3c.s3.head_object(
            Bucket=self.data_s3c.s3bucket,
            Key=fullpath
        )
        return FileMarker(fullpath, insert_time, obj['ContentLength']), running_schema

    def insert(self, rows: list[dict]) -> list[FileMarker]:
        """
        Creates one or more files in the destination folder based on the partition strategy :param rows: Rows of JSON
        data to be inserted. Must have the expected keys of the partitioning strategy and the sorting order
        """
        part_map: Dict[str, list[dict]] = {}
        for row in rows:
            part: str
            if "_partition" in row:
                part = row["_partition"]
                if not self.preserve_partition:
                    del row["_partition"]
            else:
                part = self.partition_function(row)

            if part not in part_map:
                part_map[part] = []
            part_map[part].append(row)

        running_schema = Schema()
        file_markers: list[FileMarker] = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = []
            for part, part_ref in part_map.items():
                futures.append(executor.submit(self.__insert_part, part, part_ref))

            for futures in concurrent.futures.as_completed(futures):
                result: tuple[FileMarker, Schema] = futures.result()
                # append file marker
                file_markers.append(result[0])
                # accumulate schema
                running_schema.accumulate(result[1].columns(), result[1].types())

        # Append to log
        logio = IceLogIO(self.path_safe_hostname)
        logio.append(self.log_s3c, 1, running_schema, file_markers)

        return file_markers

    def merge(self, max_file_size=10_000_000, max_file_count=10, asc=False) -> tuple[
        str | None, FileMarker | None, str | None, list[FileMarker] | None, LogMetadata | None]:
        """
        desc merge should be done often, working on recent partitions. asc merge should be done less often,
         fully optimizing partitions over time.

        Returns new_log, new_file_marker, partition, merged_file_markers, meta
        """
        logio = IceLogIO(self.path_safe_hostname)
        cur_schema, cur_files, cur_tombstones, all_log_files = logio.read_at_max_time(self.log_s3c, round(time() * 1000))

        # Group by partition
        partitions: Dict[str, list[FileMarker]] = {}
        for file in cur_files:
            partition = self.__get_file_partition(file.path)
            if partition not in partitions:
                partitions[partition] = []
            partitions[partition].append(file)

        # sort the dict
        partitions = dict(sorted(partitions.items(), key=lambda item: len(item[1]), reverse=not asc))
        for partition, file_markers in partitions.items():
            if len(file_markers) <= 1:
                continue
            # sort the items in the array by file size, asc
            sorted_file_markers = sorted(file_markers, key=lambda item: item.fileBytes)
            # aggregate until we meet the max file count or limit
            acc_bytes = 0
            acc_file_markers: list[FileMarker] = []
            for file_marker in sorted_file_markers:
                if file_marker.tombstone is not None:
                    continue
                acc_bytes += file_marker.fileBytes
                acc_file_markers.append(file_marker)
                if acc_bytes >= max_file_size or len(acc_file_markers) > 1 and len(acc_file_markers) >= max_file_count:
                    # then we merge
                    break
            if len(acc_file_markers) == 1:
                # this partition had nothing to merge
                continue

            # merge data parts
            filename = str(uuid4()) + '.parquet'
            path_parts = ['_data', partition, filename]
            if self.data_s3c.s3prefix is not None:
                path_parts = [self.data_s3c.s3prefix] + path_parts
            fullpath = '/'.join(path_parts)

            q = "COPY ({}) TO '{}' (FORMAT PARQUET, CODEC '{}', ROW_GROUP_SIZE {})".format(
                ("select * from source_files" if self.custom_merge_query is None else self.custom_merge_query).replace(
                    "source_files", "read_parquet(?, hive_partitioning=1)"),
                's3://{}/{}'.format(self.data_s3c.s3bucket, fullpath),
                self.compression_codec.value, self.row_group_size
            )

            ddb = self.get_duckdb()
            ddb.execute(q, [
                list(map(lambda x: f"s3://{self.data_s3c.s3bucket}/{x.path}", acc_file_markers))
            ])

            # get the new file size
            obj = self.data_s3c.s3.head_object(
                Bucket=self.data_s3c.s3bucket,
                Key=fullpath
            )
            merged_file_size = obj['ContentLength']

            # Now we need to get the current state of the files we just merged, and write that plus the new state
            # We can keep the current schema
            merged_log_files = list(map(lambda x: x.vir_source_log_file, acc_file_markers))
            m_schema, m_file_markers, m_tombstones = logio.read_log_forward(self.log_s3c, merged_log_files)

            # create new log file with tombstones
            acc_file_paths = list(map(lambda x: x.path, acc_file_markers))
            merged_time = round(time() * 1000)
            new_file_marker = FileMarker(fullpath, merged_time, merged_file_size)


            print("acc_file_paths", acc_file_paths);
            print("m_file_markers", list(map(lambda x: x.path, m_file_markers)));
            updated_markers = []
            for x in m_file_markers:
                tombstone = merged_time if x.path in acc_file_paths else x.tombstone
                print(f"tombstone {x.path} {tombstone} (merged_time {merged_time}) (x.path in acc_file_paths): {x.path in acc_file_paths}")
                marker = FileMarker(x.path, x.createdMS, x.fileBytes, tombstone)
                print(marker)
                updated_markers.append(marker)

            new_tombstones = list(map(lambda x: LogTombstone(x, merged_time),
                                      merged_log_files))

            new_log, meta = logio.append(
                self.log_s3c,
                1,
                m_schema,
                updated_markers + [
                    new_file_marker
                ],
                m_tombstones + new_tombstones,
                merged=True
            )

            return new_log, new_file_marker, partition, acc_file_markers, meta

        # otherwise we did not merge
        return None, None, None, [], None

    def tombstone_cleanup(self, min_age_ms: int) -> tuple[list[str], list[str], list[str]]:
        """
        Removes parquet files that are no longer active, and are older than some age. Returns the number of files deleted.

        For performance, icedb will optimistically delete files from S3, meaning that if a crash occurs during the middle of a removal then files may be left in S3 even though they are seen as deleted in the DB.

        Returns the list of log files that were cleaned, log files that were deleted, and data files that
        were deleted
        """
        logio = IceLogIO(self.path_safe_hostname)
        cleaned_log_files: list[str] = []
        deleted_log_files: list[str] = []
        deleted_data_files: list[str] = []
        now = round(time() * 1000)

        log_files_to_delete: dict[str, bool] = {}
        data_files_to_delete: dict[str, bool] = {}
        data_files_to_keep: dict[str, FileMarker] = {}
        schema = Schema()

        current_log_files = logio.get_current_log_files(self.log_s3c)
        # We only need to get merge files
        merge_log_files = list(filter(lambda x: get_log_file_info(x['Key'])[1], current_log_files))
        for file in merge_log_files:
            obj = self.log_s3c.s3.get_object(
                Bucket=self.log_s3c.s3bucket,
                Key=file['Key']
            )
            print(f"======tombstone_cleanup {file['Key']}======")
            jsonl = str(obj['Body'].read(), encoding="utf-8").split("\n")
            print("\n".join(jsonl))
            print("======")
            meta_json = json.loads(jsonl[0])
            meta = LogMetadataFromJSON(meta_json)
            expired = now - min_age_ms
            skipped_files = 0
            # Log tombstones
            if meta.tombstoneLineIndex is not None:
                for i in range(meta.tombstoneLineIndex, meta.fileLineIndex):
                    tmb = LogTombstoneFromJSON(dict(json.loads(jsonl[i])))
                    print(f"considering deleting log {tmb.path}: {tmb.createdMS} > {expired} ({tmb.createdMS - expired})")
                    if tmb.createdMS <= expired:
                        print(f"- deleting {tmb.path}")
                        log_files_to_delete[tmb.path] = True
                    else:
                        skipped_files += 1
                        print(f"- not old enough {tmb.path}")
            # File markers
            for i in range(meta.fileLineIndex, len(jsonl)):
                fm_json = dict(json.loads(jsonl[i]))
                fm = FileMarkerFromJSON(fm_json)
                print(f"considering deleting data file {fm.path}: {fm.createdMS} > {expired} ({fm.createdMS - expired})")

                if fm.createdMS <= expired and fm.tombstone is not None:
                    data_files_to_delete[fm.path] = True
                    if fm.path in data_files_to_keep:
                        del data_files_to_keep[fm.path]
                    print(f"- deleting {fm.path}")
                else:
                    skipped_files += 1
                    if fm.tombstone is None:
                        print(f"- not tombstoned {fm.path}")
                    else:
                        print(f"- not old enough {fm.path}")
                    data_files_to_keep[fm.path] = fm

            # Accumulate schema
            schema_json = dict(json.loads(jsonl[meta.schemaLineIndex]))
            schema.accumulate(list(schema_json.keys()), list(schema_json.values()))

            cleaned_log_files.append(file['Key'])
            if skipped_files > 0:
                print(f"tomstone_cleanup cancelled for {file['Key']}: {skipped_files} files could not be deleted")
                return [], [], []

        # Delete log tombstones
        for log_path in log_files_to_delete.keys():
            self.log_s3c.s3.delete_object(
                Bucket=self.log_s3c.s3bucket,
                Key=log_path
            )
            deleted_log_files.append(log_path)

        # Delete data files
        for data_path in data_files_to_delete.keys():
            self.data_s3c.s3.delete_object(
                Bucket=self.data_s3c.s3bucket,
                Key=data_path
            )
            deleted_data_files.append(data_path)



        # New log file
        new_log, _ = logio.append(
            self.log_s3c,
            1,
            schema,
            list(data_files_to_keep.values()),
            None,
            merged=True,
            timestamp=round(time()*1000)
        )
        deleted_log_files += log_files_to_delete.keys()
        deleted_data_files += data_files_to_delete
        # Delete the cleaned log files
        for path in cleaned_log_files:
            self.log_s3c.s3.delete_object(
                Bucket=self.log_s3c.s3bucket,
                Key=path
            )
        print(f"Keeping {len(data_files_to_keep)} files")

        return cleaned_log_files, deleted_log_files, deleted_data_files

    def remove_partitions(self, removal_func: PartitionRemovalFunctionType, max_files=1000) -> tuple[str | None,
    LogMetadata | None, int]:
        """
        remove_partitions is used to drop entire partitions for functionality such as TTL or user data deletion.
        The `removal_func` is provided a list of unique partitions, and must return the list of
        partitions that should be dropped.
        Those data parts will be marked with tombstones in a log-only merge.

        Returns the new log file path, the log file metadata, and the number of data files deleted

        Requires the merge lock if running concurrently.
        """

        remove_time = round(time() * 1000)

        logio = IceLogIO(self.path_safe_hostname)
        cur_schema, cur_files, cur_tombstones, all_log_files = logio.read_at_max_time(self.log_s3c, remove_time)

        # Group by partition (on alive files
        alive_files = list(filter(lambda x: x.tombstone is None, cur_files))
        partitions: Dict[str, list[FileMarker]] = {}
        for file in alive_files:
            partition = self.__get_file_partition(file.path)
            if partition not in partitions:
                partitions[partition] = []
            partitions[partition].append(file)

        partitions_to_remove = removal_func(list(partitions.keys()))
        if len(partitions_to_remove) == 0:
            # nothing to do
            return None, None, 0

        modified_log_files = {}
        updated_file_markers: list[FileMarker] = []
        deleted_parts = 0

        # Get all the file markers and log files to tombstone
        for partition in partitions_to_remove:
            if partition not in partitions:
                continue

            file_markers = partitions[partition]
            if len(file_markers) == 0:
                continue

            for file_marker in file_markers:
                deleted_parts += 1
                file_marker.tombstone = remove_time # add the tombstone
                updated_file_markers.append(file_marker)
                modified_log_files[file_marker.vir_source_log_file] = True

            if deleted_parts >= max_files:
                # We've done enough, let's break
                break

        # Log-only merge
        log_tombstones = list(map(lambda x: LogTombstone(x, remove_time), list(modified_log_files)))
        new_log, meta = logio.append(
            self.log_s3c,
            1,
            cur_schema,
            updated_file_markers,
            log_tombstones,
            merged=True
        )

        return new_log, meta, deleted_parts

    def rewrite_partition(self, target_partition: str, filter_query: str) -> tuple[str | None, LogMetadata | None, list[str]]:
        """
        For every part in a given partition, the files are rewritten after being passed through the given SQL query.
        Useful for purging data for a given user, deduplication, and more.
        New parts are created within the same partition, and old files are marked with a tombstone.
        It is CRITICAL that new columns are not created (against the known schema, not just the
        file) as the current schema is copied to the new log file, and changes will be ignored by the log.

        The target data will be at `_rows`, so for example your query might look like:

        ```
        select *
        from _rows
        where user_id != 'user_a'
        ```

        Returns the new log file path, metadata, and the list of data files that were rewritten.

        Requires the merge lock if running concurrently.
        """

        run_time = round(time() * 1000)

        logio = IceLogIO(self.path_safe_hostname)
        cur_schema, cur_files, cur_tombstones, all_log_files = logio.read_at_max_time(self.log_s3c, run_time)

        # Get alive files matching partition
        alive_files = list(filter(lambda x: x.tombstone is None, cur_files))
        rewrite_targets: list[FileMarker] = []
        for file in alive_files:
            partition = self.__get_file_partition(file.path)
            if target_partition == partition:
                rewrite_targets.append(file)

        if len(rewrite_targets) == 0:
            return None, None, []

        new_files: list[FileMarker] = []
        for old_file in rewrite_targets:
            filename = str(uuid4()) + '.parquet'
            partition = self.__get_file_partition(old_file.path)
            path_parts = ['_data', partition, filename]
            if self.data_s3c.s3prefix is not None:
                path_parts = [self.data_s3c.s3prefix] + path_parts
            fullpath = '/'.join(path_parts)

            # Copy the files through the query
            ddb = self.get_duckdb()
            ddb.execute("""
                        copy ({}) to 's3://{}/{}' (format parquet, codec '{}', row_group_size {})
                        """.format(
                filter_query.replace("_rows", "read_parquet(?)"),
                self.data_s3c.s3bucket,
                fullpath,
                self.compression_codec.value,
                self.row_group_size
            ), [f"s3://{self.data_s3c.s3bucket}/{old_file.path}"])
            write_time = round(time() * 1000)

            # get file metadata
            obj = self.data_s3c.s3.head_object(
                Bucket=self.data_s3c.s3bucket,
                Key=fullpath
            )
            new_files.append(FileMarker(fullpath, write_time, obj['ContentLength']))

        rewritten_paths = list(map(lambda x: x.path, rewrite_targets))
        updated_markers = list(map(lambda x: FileMarker(
            x.path,
            x.createdMS,
            x.fileBytes,
            run_time if x.path in rewritten_paths else x.tombstone),
                                   cur_files))

        new_tombstones = list(map(lambda x: LogTombstone(x, run_time),
                                  list(map(lambda x: x.vir_source_log_file, rewrite_targets))))

        new_log, meta = logio.append(
            self.log_s3c,
            1,
            cur_schema,
            updated_markers + new_files,
            cur_tombstones + new_tombstones,
            merged=True
        )

        return new_log, meta, list(map(lambda x: x.path, rewrite_targets))
