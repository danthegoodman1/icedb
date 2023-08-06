import json
import boto3
import botocore
from typing import Dict
from time import time


class SchemaConflictException(Exception):
    column: str
    foundTypes: list[str]
    message: str

    def __init__(self, column: str, foundTypes: list[str]):
        self.column = column
        self.foundTypes = foundTypes
        self.message = f"tried to convert schema to JSON with column '{self.column}' conflicting types: {', '.join(foundTypes)}"

class NoLogFilesException(Exception):
    column: str
    foundTypes: list[str]
    message: str

    def __init__(self):
        self.message = "no log files found"


class S3Client():
    s3: any
    s3prefix: str
    s3bucket: str
    session: any

    def __init__(
        self,
        s3prefix: str,
        s3bucket: str,
        s3region: str,
        s3endpoint: str,
        s3accesskey: str,
        s3secretkey: str
    ):
        self.s3prefix = s3prefix
        self.session = boto3.session.Session()
        self.s3 = self.session.client('s3',
            config=botocore.config.Config(s3={'addressing_style': 'path'}),
            region_name=s3region,
            endpoint_url=s3endpoint,
            aws_access_key_id=s3accesskey,
            aws_secret_access_key=s3secretkey
        )
        self.s3bucket = s3bucket


class Schema:
    """
    Accumulated schema. It is safe to pass in columns and types redundantly in the constructor. Can raise a `SchemaConflictException` if conflicting types are passed in.
    """
    d = {}

    def __init__(self):
        self.d = {}

    def accumulate(self, columns: list[str], types: list[str]):
        for i in range(len(columns)):
            col = columns[i]
            colType = types[i]
            if col in self.d and colType != self.d[col]:
                raise SchemaConflictException(col, [self.d[col], colType])
            self.d[col] = colType
        return self

    def columns(self) -> list[str]:
        cols = []
        for col in self.d.keys():
            cols.append(col)
        return cols

    def types(self) -> list[str]:
        dataTypes = []
        for col in self.d.keys():
            dataTypes.append(self.d[col])
        return dataTypes

    def pairs(self) -> list[list[str]]:
        pairs = []
        for col in self.d.keys():
            pairs.append([col, self.d[col]])
        return pairs

    def toJSON(self) -> str:
        return json.dumps(self.d)

    def __str__(self):
        return self.toJSON()

    def __repr__(self):
        return self.toJSON()


class FileMarker:
    path: str
    createdMS: int
    fileBytes: int
    tombstone: int | None

    def __init__(self, path: str, createdMS: int, fileBytes: int, tombstone: int | None = None):
        self.path = path
        self.createdMS = createdMS
        self.fileBytes = fileBytes
        self.tombstone = tombstone

    def toJSON(self) -> str:
        d = {
            "p": self.path,
            "b": self.fileBytes,
            "t": self.createdMS,
        }

        if self.tombstone is not None:
            d["tmb"] = self.tombstone

        return json.dumps(d)

    def __str__(self):
        return self.toJSON()

    def __repr__(self):
        return self.toJSON()


class LogTombstone:
    path: str
    createdMS: int

    def __init__(self, path: str, createdMS: int):
        self.path = path
        self.createdMS = createdMS

    def toJSON(self) -> str:
        return json.dumps({
            "p": self.path,
            "t": self.createdMS
        })

    def __str__(self):
        return self.toJSON()

    def __repr__(self):
        return self.toJSON()


class LogMetadata:
    version: int
    schemaLineIndex: int
    fileLineIndex: int
    tombstoneLineIndex: int | None
    timestamp: int

    def __init__(self, version: int, schemaLineIndex: int, fileLineIndex: int, tombstoneLineIndex: int = None):
        self.version = version
        self.schemaLineIndex = schemaLineIndex
        self.fileLineIndex = fileLineIndex
        self.tombstoneLineIndex = tombstoneLineIndex
        self.timestamp = round(time()*1000)

    def toJSON(self) -> str:
        d = {
            "v": self.version,
            "sch": self.schemaLineIndex,
            "f": self.fileLineIndex,
            "t": self.timestamp
        }

        if self.tombstoneLineIndex is not None:
            d["tmb"] = self.tombstoneLineIndex

        return json.dumps(d)

    def __str__(self):
        return self.toJSON()

    def __repr__(self):
        return self.toJSON()


def LogMetadataFromJSON(jsonl: dict):
    lm = LogMetadata(jsonl["v"], jsonl["sch"], jsonl["f"], jsonl["tmb"] if "tmb" in jsonl else None)
    lm.timestamp = jsonl["t"]
    return lm


class IceLogIO:
    path_safe_hostname: str

    def __init__(self, path_safe_hostname: str):
        self.path_safe_hostname = path_safe_hostname

    @staticmethod
    def reverse_read(s3client: S3Client) -> tuple[Schema, list[FileMarker], list[LogTombstone], list[str]]:
        """
        Reads the current state
        """
        s3_files: list[dict] = []
        no_more_files = False
        continuation_token = ""
        while not no_more_files:
            res = s3client.s3.list_objects_v2(
                Bucket=s3client.s3bucket,
                MaxKeys=1000,
                Prefix='/'.join([s3client.s3prefix, '_log'])
            ) if continuation_token != "" else s3client.s3.list_objects_v2(
                Bucket=s3client.s3bucket,
                MaxKeys=1000,
                Prefix='/'.join([s3client.s3prefix, '_log'])
            )
            s3_files += res['Contents']
            no_more_files = not res['IsTruncated']
            if not no_more_files:
                continuation_token = res['NextContinuationToken']

        merge_ts: int | None = None
        s3_files = sorted(s3_files, key=lambda x: x['Key'], reverse=True)
        relevant_log_files: list[str] = []
        for file in s3_files:
            file_name = file['Key'].split("/")[-1]
            us_parts = file_name.split("_")
            # Get timestamp and merge timestamp
            file_ts = int(us_parts[0])
            if merge_ts is not None and file_ts <= merge_ts:
                print("we hit a file older than the merge, aborting")
                break
            if us_parts[1] == "merged" and len(us_parts) > 3:
                print("found a merge file", file_name)
                # We found a merge file
                merge_ts = int(us_parts[2])
            relevant_log_files.append(file['Key'])

        if len(relevant_log_files) == 0:
            raise NoLogFilesException

        # Now parse files forward like normal reader (ascending)
        relevant_log_files = sorted(relevant_log_files)

        total_schema = Schema()
        alive_files: Dict[str, FileMarker] = {}
        tombstones: Dict[str, LogTombstone] = {}
        log_files: list[str] = []

        for file in relevant_log_files:
            log_files.append(file)
            obj = s3client.s3.get_object(
                Bucket=s3client.s3bucket,
                Key=file
            )
            jsonl = str(obj['Body'].read(), encoding="utf-8").split("\n")
            meta_json = json.loads(jsonl[0])
            meta = LogMetadataFromJSON(meta_json)

            # Schema
            schema = dict(json.loads(jsonl[meta.schemaLineIndex]))
            total_schema.accumulate(list(schema.keys()), list(schema.values()))

            # Log tombstones
            if meta.tombstoneLineIndex is not None:
                for i in range(meta.tombstoneLineIndex, meta.fileLineIndex):
                    tmb_dict = dict(json.loads(jsonl[i]))
                    tombstones[tmb_dict["p"]] = LogTombstone(tmb_dict["p"], int(tmb_dict["t"]))

            # Files
            for i in range(meta.fileLineIndex, len(jsonl)):
                fm_json = dict(json.loads(jsonl[i]))
                # if fm_json["p"] in alive_files and "tmb" in fm_json:
                #     # Not alive, remove
                #     del alive_files[fm_json["p"]]
                #     continue

                # Otherwise add if not exists
                fm = FileMarker(fm_json["p"], int(fm_json["t"]), int(fm_json["b"]),
                                fm_json["tmb"] if "tmb" in fm_json else None)
                # if fm_json["p"] not in alive_files:
                alive_files[fm_json["p"]] = fm

        return total_schema, list(alive_files.values()), list(tombstones.values()), log_files

    @staticmethod
    def read_at_max_time(s3client: S3Client, timestamp: int) -> tuple[Schema, list[FileMarker], list[LogTombstone], list[str]]:
        """
        Read the current state of the log up to a given timestamp
        """
        s3_files: list[dict] = []
        no_more_files = False
        continuation_token = ""
        while not no_more_files:
            res = s3client.s3.list_objects_v2(
                Bucket=s3client.s3bucket,
                MaxKeys=1000,
                Prefix='/'.join([s3client.s3prefix, '_log'])
            ) if continuation_token != "" else s3client.s3.list_objects_v2(
                Bucket=s3client.s3bucket,
                MaxKeys=1000,
                Prefix='/'.join([s3client.s3prefix, '_log'])
            )
            s3_files += res['Contents']
            no_more_files = not res['IsTruncated']
            if not no_more_files:
                continuation_token = res['NextContinuationToken']

        if len(s3_files) == 0:
            raise NoLogFilesException

        total_schema = Schema()
        alive_files: Dict[str, FileMarker] = {}
        tombstones: Dict[str, LogTombstone] = {}
        log_files: list[str] = []

        for file in s3_files:
            log_files.append(file['Key'])
            obj = s3client.s3.get_object(
                Bucket=s3client.s3bucket,
                Key=file['Key']
            )
            jsonl = str(obj['Body'].read(), encoding="utf-8").split("\n")
            meta_json = json.loads(jsonl[0])
            meta = LogMetadataFromJSON(meta_json)
            if meta.timestamp > timestamp:
                pass

            # Schema
            schema = dict(json.loads(jsonl[meta.schemaLineIndex]))
            total_schema.accumulate(list(schema.keys()), list(schema.values()))

            # Log tombstones
            if meta.tombstoneLineIndex is not None:
                for i in range(meta.tombstoneLineIndex, meta.fileLineIndex):
                    tmb_dict = dict(json.loads(jsonl[i]))
                    tombstones[tmb_dict["p"]] = LogTombstone(tmb_dict["p"], int(tmb_dict["t"]))

            # Files
            for i in range(meta.fileLineIndex, len(jsonl)):
                fm_json = dict(json.loads(jsonl[i]))
                # if fm_json["p"] in alive_files and "tmb" in fm_json:
                #     # Not alive, remove
                #     del alive_files[fm_json["p"]]
                #     continue

                # Otherwise add if not exists
                fm = FileMarker(fm_json["p"], int(fm_json["t"]), int(fm_json["b"]), fm_json["tmb"] if "tmb" in fm_json else None)
                # if fm_json["p"] not in alive_files:
                alive_files[fm_json["p"]] = fm

        if len(log_files) == 0:
            raise NoLogFilesException

        return total_schema, list(alive_files.values()), list(tombstones.values()), log_files

    def append(self, s3client: S3Client, version: int, schema: Schema, files: list[FileMarker], tombstones: list[LogTombstone] = None, merge_ts: int = None) -> tuple[str, LogMetadata]:
        """
        Creates a new log file in S3, in the order of version, schema, tombstones?, files
        """
        log_file_lines: list[str] = []
        meta = LogMetadata(version, 1, 2 if tombstones is None or len(tombstones) == 0 else 2+len(tombstones), None if tombstones is None or len(tombstones) == 0 else 2)
        log_file_lines.append(meta.toJSON())
        log_file_lines.append(schema.toJSON())
        if tombstones is not None:
            for tmb in tombstones:
                log_file_lines.append(tmb.toJSON())
        for fileMarker in files:
            log_file_lines.append(fileMarker.toJSON())

        file_id = f"{meta.timestamp}"
        if merge_ts is not None:
            file_id += f"_merge_{merge_ts}"
        file_id += f"_{self.path_safe_hostname}"

        # Upload the file to S3
        file_key = "/".join([s3client.s3prefix, '_log', file_id+'.jsonl'])
        s3client.s3.put_object(
            Body=bytes('\n'.join(log_file_lines), 'utf-8'),
            Bucket=s3client.s3bucket,
            Key=file_key
        )
        return file_key, meta
