from typing import List, Callable, Dict
import duckdb
from uuid import uuid4
import pandas as pd
from log import IceLogIO, Schema, LogMetadata, S3Client, FileMarker, LogTombstone
from time import time

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
            row_group_size: int = 122_880
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

        if s3_use_path:
            self.ddb.execute("SET s3_url_style='path'")
        if duckdb_ext_dir is not None:
            self.ddb.execute(f"SET extension_directory='{duckdb_ext_dir}'")

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
            part_rows = part_map[part]

            # use a DF for inserting into duckdb
            first_row = self.format_row(part_rows[0].copy())  # need to make copy so we don't modify
            first_row['_row_id'] = str(uuid4()) if self.unique_row_key is None else part_rows[0][self.unique_row_key]
            for key in first_row:
                # convert everything to array
                first_row[key] = [first_row[key]]
            df = pd.DataFrame(first_row)
            if len(part_rows) > 1:
                # we need to add more rows
                for row in part_rows[1:]:
                    new_row = self.format_row(row.copy())  # need to make copy so we don't modify
                    new_row['_row_id'] = str(uuid4()) if self.unique_row_key is None else row[self.unique_row_key]
                    df.loc[len(df)] = new_row

            # get schema
            self.ddb.execute("describe select * from df")
            schema_df = self.ddb.df()
            running_schema.accumulate(schema_df['column_name'].tolist(), schema_df['column_type'].tolist())

            # copy to parquet file
            self.ddb.execute(
                f"copy (select * from df order by {','.join(self.sort_order)}) to 's3://{self.s3c.s3bucket}/{fullpath}' (FORMAT PARQUET, ROW_GROUP_SIZE {self.row_group_size})")

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

        Returns the number of files merged.
        """
        logio = IceLogIO(self.path_safe_hostname)
        cur_schema, cur_files, cur_tombstones, all_log_files = logio.reverse_read(self.s3c)

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
                acc_bytes += file_marker.fileBytes
                acc_file_markers.append(file_marker)
                if acc_bytes >= max_file_size and len(acc_file_markers) > 1 and len(acc_file_markers) >= max_file_count:
                    # then we merge
                    break
            if len(acc_file_markers) > 1:
                # merge data parts
                filename = str(uuid4()) + '.parquet'
                path_parts = ['_data', partition, filename]
                if self.s3c.s3prefix is not None:
                    path_parts = [self.s3c.s3prefix] + path_parts
                fullpath = '/'.join(path_parts)

                q = "COPY ({}) TO '{}' (FORMAT PARQUET, ROW_GROUP_SIZE {})".format(
                    ("select * from source_files" if custom_merge_query is None else custom_merge_query).replace(
                        "source_files", "read_parquet(?, hive_partitioning=1)"),
                    's3://{}/{}'.format(self.s3c.s3bucket, fullpath),
                    self.row_group_size
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

                # create new log file with tombstones
                acc_file_paths = list(map(lambda x: x.path, acc_file_markers))
                now = round(time() * 1000)
                new_file_marker = FileMarker(fullpath, now, merged_file_size)

                updated_markers = list(map(lambda x: FileMarker(
                    x.path,
                    x.createdMS,
                    x.fileBytes,
                    now if x.path in acc_file_paths else x.tombstone),
                                           file_markers))

                new_tombstones = list(map(lambda x: LogTombstone(x, now), filter(lambda x: x not in cur_tombstones, all_log_files)))

                new_log, meta = logio.append(
                    self.s3c,
                    1,
                    cur_schema,
                    updated_markers + [
                        new_file_marker
                    ],
                    cur_tombstones + new_tombstones,
                    merge_ts=int(all_log_files[-1].split("/")[-1].split("_")[0])
                )

                return new_log, new_file_marker, partition, acc_file_markers, meta

        # otherwise we did not merge
        return None, None, None, None, None

    def get_files(self, gte_part: str, lte_part: str) -> List[str]:
        pass

    def tombstone_cleanup(self, min_age_ms: int, partition_prefix: str = None, limit=10) -> str:
        """
        Removes parquet files that are no longer active, and are older than some age. Returns the number of files deleted.

        For performance, icedb will optimistically delete files from S3, meaning that if a crash occurs during the middle of a removal then files may be left in S3 even though they are seen as deleted in the DB.

        The best mitigation for this is multiple small removal operations.
        """
        pass
