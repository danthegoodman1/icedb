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


class FileMarker:
    path: str
    createdMS: int
    fileBytes: int
    tombstone: str | None

    def __init__(self, path: str, createdMS: int, fileBytes: int, tombstone: str | None = None):
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


def LogMetadataFromJSON(jsonl: dict):
    lm = LogMetadata(jsonl["v"], jsonl["sch"], jsonl["f"], jsonl["tmb"] if "tmb" in jsonl else None)
    lm.timestamp = jsonl["t"]
    return lm


class IceLogIO:
    fileSafeHostname: str

    def __init__(self, fileSafeHostname: str):
        self.fileSafeHostname = fileSafeHostname

    def readAtMaxTime(self, s3client: S3Client, timestamp: int) -> tuple[Schema, list[FileMarker], list[LogTombstone]]:
        '''
        Read the current state of the log up to a given timestamp
        '''
        logFiles = s3client.s3.list_objects_v2(
            Bucket=s3client.s3bucket,
            MaxKeys=1000,
            Prefix='/'.join([s3client.s3prefix, '_log'])
        )

        totalSchema = Schema()
        aliveFiles: Dict[str, FileMarker] = {}
        tombstones: Dict[str, LogTombstone] = {}

        for file in logFiles['Contents']:
            obj = s3client.s3.get_object(
                Bucket=s3client.s3bucket,
                Key=file['Key']
            )
            jsonl = str(obj['Body'].read(), encoding="utf-8").split("\n")
            metaJSON = json.loads(jsonl[0])
            meta = LogMetadataFromJSON(metaJSON)
            if meta.timestamp > timestamp:
                pass

            # Schema
            schema = dict(json.loads(jsonl[meta.schemaLineIndex]))
            totalSchema.accumulate(list(schema.keys()), list(schema.values()))

            # Log tombstones
            if meta.tombstoneLineIndex != None:
                for i in range(meta.tombstoneLineIndex, meta.fileLineIndex):
                    tmbDict = dict(json.loads(jsonl[i]))
                    if tmbDict["p"] not in tombstones:
                        tombstones[tmbDict["p"]] = LogTombstone(tmbDict["p"], tmbDict["t"])

            # Files
            for i in range(meta.fileLineIndex, len(jsonl)):
                fmJSON = dict(json.loads(jsonl[i]))
                if fmJSON["p"] in aliveFiles and "tmb" in fmJSON:
                    # Not alive, remove
                    del aliveFiles[fmJSON["p"]]
                    continue

                # Otherwise add if not exists
                fm = FileMarker(fmJSON["p"], fmJSON["t"], fmJSON["b"], fmJSON["tmb"] if "tmb" in fmJSON else None)
                if fmJSON["p"] not in aliveFiles:
                    aliveFiles[fmJSON["p"]] = fm

        return totalSchema, list(aliveFiles.values()), list(tombstones.values())

    def append(self, s3client: S3Client, version: int, schema: Schema, files: list[FileMarker], tombstones: list[LogTombstone] | None = None) -> str:
        """
        Creates a new log file in S3, in the order of version, schema, tombstones?, files
        """
        logFileLines: list[str] = []
        meta = LogMetadata(version, 1, 2 if tombstones == None else 2+len(tombstones), None if tombstones == None else 2)
        logFileLines.append(meta.toJSON())
        logFileLines.append(schema.toJSON())
        if tombstones != None:
            for tmb in tombstones:
                logFileLines.append(tmb.toJSON())
        for fileMarker in files:
            logFileLines.append(fileMarker.toJSON())

        fileID = f"{meta.timestamp}_{self.fileSafeHostname}"

        # Upload the file to S3
        fileKey = "/".join([s3client.s3prefix, '_log', fileID+'.jsonl'])
        s3client.s3.put_object(
            Body=bytes('\n'.join(logFileLines), 'utf-8'),
            Bucket=s3client.s3bucket,
            Key=fileKey
        )
        return fileKey
