from typing import List, Callable, Dict
import duckdb
from uuid import uuid4
from .log import (IceLogIO, Schema, LogMetadata, S3Client, FileMarker, LogTombstone, get_log_file_info,
                 LogMetadataFromJSON, LogTombstoneFromJSON, FileMarkerFromJSON)
from time import time
import json
from enum import Enum
import pyarrow as pa
from copy import deepcopy


class CompressionCodec(Enum):
    UNCOMPRESSED = "UNCOMPRESSED"
    SNAPPY = "SNAPPY"
    ZSTD = "ZSTD"
    GZIP = "GZIP"


PartitionFunctionType = Callable[[dict], str]
FormatRowType = Callable[[dict], dict]

class IceDBv3:
    partition_function: PartitionFunctionType
    sort_order: List[str]
    format_row: FormatRowType
    ddb: duckdb
    s3c: S3Client
    unique_row_key: str | None
    custom_merge_query: str | None
    row_group_size: int
    path_safe_hostname: str
    compression_codec: CompressionCodec
    auto_copy: bool

    def __init__(
            self,
            partition_function: PartitionFunctionType,
            sort_order: List[str],
            format_row: FormatRowType,
            s3_region: str,
            s3_access_key: str,
            s3_secret_key: str,
            s3_endpoint: str,
            s3_client: S3Client,
            path_safe_hostname: str,
            s3_use_path: bool = False,
            duckdb_ext_dir: str = None,
            custom_merge_query: str = None,
            unique_row_key: str = None,
            row_group_size: int = 122_880,
            compression_codec: CompressionCodec = CompressionCodec.SNAPPY,
            auto_copy: bool = True
    ):
        self.partition_function = partition_function
        self.sort_order = sort_order
        self.format_row = format_row
        self.row_group_size = row_group_size
        self.path_safe_hostname = path_safe_hostname
        self.unique_row_key = unique_row_key
        self.s3c = s3_client
        self.custom_merge_query = custom_merge_query

        self.ddb = duckdb.connect(":memory:")
        self.ddb.execute("install httpfs")
        self.ddb.execute("load httpfs")
        self.ddb.execute(f"SET s3_region='{s3_region}'")
        self.ddb.execute(f"SET s3_access_key_id='{s3_access_key}'")
        self.ddb.execute(f"SET s3_secret_access_key='{s3_secret_key}'")
        self.ddb.execute(f"SET s3_endpoint='{s3_endpoint.split('://')[1]}'")
        self.ddb.execute(f"SET s3_use_ssl={'false' if 'http://' in s3_endpoint else 'true'}")
        if not isinstance(compression_codec, CompressionCodec):
            raise AttributeError(f"invalid compression codec '{compression_codec}', must be one of type CompressionCodec")

        self.compression_codec = compression_codec

        if s3_use_path:
            self.ddb.execute("SET s3_url_style='path'")
        if duckdb_ext_dir is not None:
            self.ddb.execute(f"SET extension_directory='{duckdb_ext_dir}'")

    def __format_lambda(self, row):
        row = self.format_row(row if self.auto_copy is False else deepcopy(row))
        row['_row_id'] = str(uuid4()) if self.unique_row_key is None else row[self.unique_row_key]
        return row

    def __format_lambda_str(self, row):
        """
        A version of format_lambda that just uses a string for
        performance purposes when calculating schema
        """
        row = self.format_row(row if self.auto_copy is False else deepcopy(row))
        row['_row_id'] = "" if self.unique_row_key is None else row[self.unique_row_key]
        return row

    def get_schema(self, rows: list[dict]):
        """
        Creates one or more files in the destination folder based on the partition strategy :param rows: Rows of JSON
        data to be inserted. Must have the expected keys of the partitioning strategy and the sorting order
        """
        running_schema = Schema()

        arw = pa.Table.from_pylist(list(map(self.__format_lambda_str, rows)))

        # get schema
        self.ddb.execute("describe select * from arw")
        schema_arrow = self.ddb.arrow()
        running_schema.accumulate(list(map(lambda x: str(x), schema_arrow.column('column_name'))), list(map(lambda x:
         str(x), schema_arrow.column('column_type'))))
        return running_schema

    def insert(self, rows: list[dict]) -> list[FileMarker]:
        """
        Creates one or more files in the destination folder based on the partition strategy :param rows: Rows of JSON
        data to be inserted. Must have the expected keys of the partitioning strategy and the sorting order
        """
        part_map: Dict[str, list[dict]] = {}
        for row in rows:
            # merge the rows into same parts
            part = self.partition_function(row)
            if part not in part_map:
                part_map[part] = []
            part_map[part].append(row)

        running_schema = Schema()
        file_markers: list[FileMarker] = []

        for part in part_map:
            # upload parquet file
            filename = str(uuid4()) + '.parquet'
            path_parts = ['_data', part, filename]
            if self.s3c.s3prefix is not None:
                path_parts = [self.s3c.s3prefix] + path_parts
            fullpath = '/'.join(path_parts)
            s = time()
            part_rows = list(map(self.__format_lambda, part_map[part]))
            # for item in part_rows:
            #     self.format_row(item)
            #     item['_row_id'] = str(uuid4()) if self.unique_row_key is None else item[self.unique_row_key]

            # py arrow table for inserting into duckdb
            arw = pa.Table.from_pylist(part_rows)
            # print(arw)

            # get schema
            self.ddb.execute("describe select * from arw")
            schema_arrow = self.ddb.arrow()
            running_schema.accumulate(list(map(lambda x: str(x), schema_arrow.column('column_name'))),
                                      list(map(lambda x: str(x), schema_arrow.column('column_type'))))

            # copy to parquet file
            self.ddb.execute(
                f"copy (select * from arw order by {','.join(self.sort_order)}) to 's3://{self.s3c.s3bucket}/"
                f"{fullpath}' (FORMAT PARQUET, CODEC '{self.compression_codec.value}', ROW_GROUP_SIZE"
                f" {self.row_group_size})")

            # get file metadata
            obj = self.s3c.s3.head_object(
                Bucket=self.s3c.s3bucket,
                Key=fullpath
            )
            file_markers.append(FileMarker(fullpath, round(time() * 1000), obj['ContentLength']))

        # Append to log
        logio = IceLogIO(self.path_safe_hostname)
        log_path, log_meta = logio.append(self.s3c, 1, running_schema, file_markers)

        return file_markers

    def merge(self, max_file_size=10_000_000, max_file_count=10, asc=False,
              custom_merge_query: str = None) -> tuple[
        str | None, FileMarker | None, str | None, list[FileMarker] | None, LogMetadata | None]:
        """
        desc merge should be fast, working on active partitions. asc merge should be slow and in background,
        slowly fully optimizes partitions over time.

        Returns new_log, new_file_marker, partition, merged_file_markers, meta
        """
        logio = IceLogIO(self.path_safe_hostname)
        cur_schema, cur_files, cur_tombstones, all_log_files = logio.read_at_max_time(self.s3c, round(time() * 1000))

        # Group by partition
        partitions: Dict[str, list[FileMarker]] = {}
        for file in cur_files:
            file_path = file.path
            if self.s3c.s3prefix is not None:
                file_path = file_path.removeprefix(self.s3c.s3prefix + "/_data/")
            path_parts = file_path.split("/")
            # remove the file name
            partition = '/'.join(path_parts[:-1])
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
            if len(acc_file_markers) > 1:
                # merge data parts
                filename = str(uuid4()) + '.parquet'
                path_parts = ['_data', partition, filename]
                if self.s3c.s3prefix is not None:
                    path_parts = [self.s3c.s3prefix] + path_parts
                fullpath = '/'.join(path_parts)

                q = "COPY ({}) TO '{}' (FORMAT PARQUET, CODEC '{}', ROW_GROUP_SIZE {})".format(
                    ("select * from source_files" if custom_merge_query is None else custom_merge_query).replace(
                        "source_files", "read_parquet(?, hive_partitioning=1)"),
                    's3://{}/{}'.format(self.s3c.s3bucket, fullpath),
                    self.compression_codec.value, self.row_group_size
                )

                self.ddb.execute(q, [
                    list(map(lambda x: f"s3://{self.s3c.s3bucket}/{x.path}", acc_file_markers))
                ])

                # get the new file size
                obj = self.s3c.s3.head_object(
                    Bucket=self.s3c.s3bucket,
                    Key=fullpath
                )
                merged_file_size = obj['ContentLength']

                # Now we need to get the current state of the files we just merged, and write that plus the new state
                # We can keep the current schema
                merged_log_files = list(map(lambda x: x.vir_source_log_file, acc_file_markers))
                m_schema, m_file_markers, m_tombstones = logio.read_log_forward(self.s3c, merged_log_files)

                # create new log file with tombstones
                acc_file_paths = list(map(lambda x: x.path, acc_file_markers))
                merged_time = round(time() * 1000)
                new_file_marker = FileMarker(fullpath, merged_time, merged_file_size)

                updated_markers = list(map(lambda x: FileMarker(
                    x.path,
                    x.createdMS,
                    x.fileBytes,
                    merged_time if x.path in acc_file_paths else x.tombstone),
                                           m_file_markers))


                new_tombstones = list(map(lambda x: LogTombstone(x, merged_time),
                                          merged_log_files))

                new_log, meta = logio.append(
                    self.s3c,
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

        current_log_files = logio.get_current_log_files(self.s3c)
        # We only need to get merge files
        merge_log_files = list(filter(lambda x: get_log_file_info(x['Key'])[1], current_log_files))
        for file in merge_log_files:
            obj = self.s3c.s3.get_object(
                Bucket=self.s3c.s3bucket,
                Key=file['Key']
            )
            jsonl = str(obj['Body'].read(), encoding="utf-8").split("\n")
            meta_json = json.loads(jsonl[0])
            meta = LogMetadataFromJSON(meta_json)

            # Log tombstones
            log_files_to_delete: list[str] = []
            if meta.tombstoneLineIndex is not None:
                for i in range(meta.tombstoneLineIndex, meta.fileLineIndex):
                    tmb = LogTombstoneFromJSON(dict(json.loads(jsonl[i])))
                    if tmb.createdMS <= now - min_age_ms:
                        log_files_to_delete.append(tmb.path)

            # File markers
            file_markers: list[FileMarker] = []
            for i in range(meta.fileLineIndex, len(jsonl)):
                fm_json = dict(json.loads(jsonl[i]))
                fm = FileMarkerFromJSON(fm_json)
                file_markers.append(fm)

            # Delete log tombstones
            for log_path in log_files_to_delete:
                self.s3c.s3.delete_object(
                    Bucket=self.s3c.s3bucket,
                    Key=log_path
                )

            # Delete data tombstones
            file_markers_to_delete: list[FileMarker] = list(filter(lambda x: x.createdMS <= now - min_age_ms and
                                                                             x.tombstone is not None, file_markers))
            for data_path in file_markers_to_delete:
                self.s3c.s3.delete_object(
                    Bucket=self.s3c.s3bucket,
                    Key=data_path.path
                )

            # Upsert log file
            schema = Schema()
            schema_json = dict(json.loads(jsonl[meta.schemaLineIndex]))
            schema.accumulate(list(schema_json.keys()), list(schema_json.values()))
            new_log, _ = logio.append(
                self.s3c,
                1,
                schema,
                list(filter(lambda x: x.tombstone is None or x.createdMS > now - min_age_ms, file_markers)),
                None,
                merged=True,
                timestamp=meta.timestamp
            )
            cleaned_log_files.append(file['Key'])
            deleted_log_files += log_files_to_delete
            deleted_data_files += list(map(lambda x: x.path, file_markers_to_delete))

        return cleaned_log_files, deleted_log_files, deleted_data_files
