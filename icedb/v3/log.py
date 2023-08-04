import json
import boto3
import botocore
from ksuid import ksuid


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

    def __init__(self, columns: list[str], types: list[str]):
        for i in range(len(columns)):
            col = columns[i]
            colType = types[i]
            if col in self.d and colType != self.d[col]:
                raise SchemaConflictException(col, [self.d[col], colType])
            self.d[col] = colType

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
    bytes: int
    tombstone: str | None

    def __init__(self, path: str, createdMS: int, bytes: int, tombstone: str | None = None):
        self.path = path
        self.createdMS = createdMS
        self.bytes = bytes
        self.tombstone = tombstone

    def toJSON(self) -> str:
        d = {
            "p": self.path,
            "b": self.bytes,
            "t": self.createdMS,
        }

        if self.tombstone is not None:
            d["tmb"] = self.tombstone

        return json.dumps(d)


class FileTombstone:
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

    def __init__(self, version: int, schemaLineIndex: int, fileLineIndex: int, tombstoneLineIndex: int | None):
        self.version = version
        self.schemaLineIndex = schemaLineIndex
        self.fileLineIndex = fileLineIndex
        self.tombstoneLineIndex = tombstoneLineIndex

    def toJSON(self) -> str:
        d = {
            "v": self.version,
            "sch": self.schemaLineIndex,
            "f": self.fileLineIndex
        }

        if self.tombstoneLineIndex is not None:
            d["tmb"] = self.tombstoneLineIndex

        return json.dumps(d)
    
    def fromJSON(jsonl: str):
        return LogMetadata(jsonl["v"], jsonl["sch"], jsonl["f"], jsonl["tmb"] if "tmb" in jsonl else None)


class IceLogIO:
    def readAtMaxTime(self, s3client: S3Client, timestamp: int) -> tuple[Schema, list[FileMarker], list[FileTombstone] | None]:
        '''
        Read the current state of the log up to a given timestamp
        '''
        logFiles = s3client.s3.list_objects_v2(
            Bucket=s3client.s3bucket,
            MaxKeys=1000,
            Prefix=s3client.s3prefix
        )

        aliveFiles = {}
        schema = {}
        tombstones = {}

        for file in logFiles['Contents']:
            obj = s3client.s3.get_object(
                Bucket=s3client.s3bucket,
                Key=file['Key']
            )
            jsonl: str = str(obj['Body'].read(), encoding="utf-8")
            jsonl = jsonl.split("\n")
            metaJSON = json.loads(jsonl[0])
            meta = LogMetadata.fromJSON(metaJSON)
        pass

    def append(self, s3client: S3Client, version: int, schema: Schema, files: list[FileMarker], tombstones: list[FileTombstone] | None = None) -> str:
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

        fileID = ksuid().__str__()

        # Upload the file to S3
        fileKey = f"{s3client.s3prefix}/{fileID}.jsonl"
        s3client.s3.put_object(
            Body=bytes('\n'.join(logFileLines), 'utf-8'),
            Bucket=s3client.s3bucket,
            Key=fileKey
        )
        return fileKey
