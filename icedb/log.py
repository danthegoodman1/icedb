import json
import boto3
import botocore
from typing import Dict
from time import time


class SchemaConflictException(Exception):
    column: str
    current_type: str
    new_type: str
    message: str

    def __init__(self, column: str, current_type: str, new_type: str):
        self.column = column
        self.current_type = current_type
        self.new_type = new_type
        self.message = (f"tried to convert schema to JSON with column '{self.column}' conflicting types: "
                        f"{', '.join([current_type, new_type])}")
    def __str__(self):
        return self.message

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

    def accumulate(self, columns: list[str], types: list[str]) -> bool:
        added = True
        for i in range(len(columns)):
            col = columns[i]
            colType = types[i]
            if col in self.d:
                added = False
                if colType != self.d[col]:
                    raise SchemaConflictException(col, self.d[col], colType)
            self.d[col] = colType
        return added

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

    def pairs(self):
        return self.d.items()

    def toJSON(self) -> str:
        return json.dumps(self.d)

    def __str__(self):
        return self.toJSON()

    def __repr__(self):
        return self.toJSON()

    def __getitem__(self, item):
        return self.d[item]

    def __contains__(self, item):
        return item in self.d


class FileMarker:
    path: str
    createdMS: int
    fileBytes: int
    tombstone: int | None

    # Only used for reading state, not included in serialization
    vir_source_log_file: str | None

    def __init__(self, path: str, createdMS: int, fileBytes: int, tombstone: int = None):
        self.path = path
        self.createdMS = createdMS
        self.fileBytes = fileBytes
        self.tombstone = tombstone
        self.vir_source_log_file = None

    def json(self) -> str:
        d = {
            "p": self.path,
            "b": self.fileBytes,
            "t": self.createdMS,
        }

        if self.tombstone is not None:
            d["tmb"] = self.tombstone

        return json.dumps(d)

    def __str__(self):
        if self.vir_source_log_file is not None:
            j = json.loads(self.json())
            j["vir_source_log_file"] = self.vir_source_log_file
            return json.dumps(j)
        else:
            return self.json()

    def __repr__(self):
        if self.vir_source_log_file is not None:
            j = json.loads(self.json())
            j["vir_source_log_file"] = self.vir_source_log_file
            return json.dumps(j)
        else:
            return self.json()

def FileMarkerFromJSON(jsonl: dict):
    fm = FileMarker(jsonl["p"], int(jsonl["t"]), int(jsonl["b"]),
                    jsonl["tmb"] if "tmb" in jsonl else None)
    return fm

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

def LogTombstoneFromJSON(jsonl: dict):
    lt = LogTombstone(jsonl["p"], jsonl["t"])
    return lt


class LogMetadata:
    version: int
    schemaLineIndex: int
    fileLineIndex: int
    tombstoneLineIndex: int | None
    timestamp: int

    def __init__(self, version: int, schemaLineIndex: int, fileLineIndex: int, tombstoneLineIndex: int = None,
                 timestamp: int = None):
        self.version = version
        self.schemaLineIndex = schemaLineIndex
        self.fileLineIndex = fileLineIndex
        self.tombstoneLineIndex = tombstoneLineIndex
        self.timestamp = timestamp if timestamp is not None else round(time()*1000)

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

    def read_log_forward(self, s3client: S3Client, s3_files: list[str]) -> tuple[Schema, list[FileMarker],
    list[LogTombstone]]:
        """
        Reads the current state of the log for a given set of files, not meant to be used externally.

        Returns the schema, file markers, log tombstones, and the list of log files read.
        """
        total_schema = Schema()
        file_markers: Dict[str, FileMarker] = {}
        tombstones: Dict[str, LogTombstone] = {}
        log_files: list[str] = []
        # ensure they are sorted
        s3_files = sorted(s3_files)

        for file in s3_files:
            log_files.append(file)
            obj = s3client.s3.get_object(
                Bucket=s3client.s3bucket,
                Key=file
            )
            print(f"======read_log_forward {file}======")
            jsonl = str(obj['Body'].read(), encoding="utf-8").split("\n")
            print("\n".join(jsonl))
            print("======")
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

                # Otherwise add if not exists
                fm = FileMarker(fm_json["p"], int(fm_json["t"]), int(fm_json["b"]),
                                fm_json["tmb"] if "tmb" in fm_json else None)
                fm.vir_source_log_file = file
                file_markers[fm_json["p"]] = fm

        if len(log_files) == 0:
            raise NoLogFilesException

        return total_schema, list(file_markers.values()), list(tombstones.values())

    def get_current_log_files(self, s3client: S3Client) -> list[dict]:
        """
        Returns the list of known log files as S3 object dictionaries
        """
        s3_files: list[dict] = []
        no_more_files = False
        continuation_token = ""
        while not no_more_files:
            res = s3client.s3.list_objects_v2(
                Bucket=s3client.s3bucket,
                MaxKeys=1000,
                Prefix='/'.join([s3client.s3prefix, '_log']),
                ContinuationToken=continuation_token
            ) if continuation_token != "" else s3client.s3.list_objects_v2(
                Bucket=s3client.s3bucket,
                MaxKeys=1000,
                Prefix='/'.join([s3client.s3prefix, '_log'])
            )
            if 'Contents' not in res:
                return []
            s3_files += res['Contents']
            no_more_files = not res['IsTruncated']
            if not no_more_files:
                continuation_token = res['NextContinuationToken']

        if len(s3_files) == 0:
            raise NoLogFilesException

        return s3_files

    def read_at_max_time(self, s3client: S3Client, timestamp: int) -> tuple[Schema, list[FileMarker],
    list[LogTombstone], list[str]]:
        """
        Read the current state of the log up to a given timestamp.

        Returns the schema, file markers, log tombstones, and the list of log files read.
        """
        s3_files = self.get_current_log_files(s3client)

        # Filter out files that are too old
        s3_files = list(filter(lambda x: get_log_file_info(x['Key'])[0] < timestamp, s3_files))

        if len(s3_files) == 0:
            raise NoLogFilesException

        log_files = list(map(lambda x: x['Key'], s3_files))
        schema, file_markers, log_tombstones = self.read_log_forward(s3client, log_files)
        return schema, file_markers, log_tombstones, log_files

    def append(self, s3client: S3Client, version: int, schema: Schema, files: list[FileMarker], tombstones: list[
        LogTombstone] = None, merged = False, timestamp: int = None) -> tuple[str, LogMetadata]:
        """
        Creates a new log file in S3, in the order of version, schema, tombstones?, files
        """
        log_file_lines: list[str] = []
        meta = LogMetadata(version, 1, 2 if tombstones is None or len(tombstones) == 0 else 2+len(tombstones),
                           None if tombstones is None or len(tombstones) == 0 else 2, timestamp=timestamp)
        log_file_lines.append(meta.toJSON())
        log_file_lines.append(schema.toJSON())
        if tombstones is not None:
            for tmb in tombstones:
                log_file_lines.append(tmb.toJSON())
        for fileMarker in files:
            log_file_lines.append(fileMarker.json())

        file_id = f"{meta.timestamp}"
        if merged:
            file_id += "_m"
        file_id += f"_{self.path_safe_hostname}"

        # Upload the file to S3
        file_key = "/".join([s3client.s3prefix, '_log', file_id+'.jsonl'])
        print(f"======append {file_key}======")
        print("\n".join(log_file_lines))
        print("======")
        s3client.s3.put_object(
            Body=bytes('\n'.join(log_file_lines), 'utf-8'),
            Bucket=s3client.s3bucket,
            Key=file_key
        )
        return file_key, meta

def get_log_file_info(file_name: str) -> tuple[int, bool]:
    """
    Returns the timestamp of the file, and optionally the merge timestamp if it exists
    """
    file_name = file_name.split("/")[-1]
    name_parts = file_name.split("_")
    # Get timestamp and merge
    file_ts = int(name_parts[0])
    merged = False
    if len(name_parts) > 2 and name_parts[1] == "m":
        merged = True
    return file_ts, merged
