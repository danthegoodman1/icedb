import json


class SchemaConflictException(Exception):
    column: str
    foundTypes: list[str]
    message: str

    def __init__(self, column: str, foundTypes: list[str]):
        self.column = column
        self.foundTypes = foundTypes
        self.message = f"tried to convert schema to JSON with column '{self.column}' conflicting types: {', '.join(foundTypes)}"


class Schema:
    """
    Accumulated schema. It is safe to pass in columns and types redundantly in the constructor.
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
    tombstone: int | None

    def __init__(self, path: str, createdMS: int, bytes: int, tombstone: str | None):
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
    schemaStartLine: int
    fileStartLine: int
    tombstoneStartLine: int | None

    def __init__(self, version: int, schemaStartLine: int, fileStartLine: int, tombstoneStartLine: int | None):
        self.version = version
        self.schemaStartLine = schemaStartLine
        self.fileStartLine = fileStartLine
        self.tombstoneStartLine = tombstoneStartLine

    def toJSON(self) -> str:
        d = {
            "v": self.version,
            "sch": self.schemaStartLine,
            "f": self.fileStartLine
        }

        if self.tombstoneStartLine is not None:
            d["tmb"] = self.tombstoneStartLine

        return json.dumps(d)


class IceLog:

    path: str

    def __init__(
        self,
        path: str
    ):
        self.path = path

    def readAtMaxTime(timestamp: int):
        pass

    def append(version: int, schema: Schema, files: list[FileMarker], tombstones: list[FileTombstone]):
        """
        Creates a new log file in S3
        """
