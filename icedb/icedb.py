import partition_strategy
from typing import List, Callable
import duckdb

PartitionFunctionType = Callable[[dict], List[List[str]]]

class IceDB:

    partStrategy: partition_strategy.PartitionFunctionType
    sortOrder: List[str]
    ddb: duckdb

    def __init__(self, partitionStrategy: partition_strategy.PartitionFunctionType, sortingOrder: List[str]):
        self.partStrategy = partitionStrategy
        self.sortOrder = sortingOrder

        self.ddb = duckdb.connect(":memory:")
        self.ddb.execute("install httpfs")
        self.ddb.execute("load httpfs")
        self.ddb.execute("install parquet")
        self.ddb.execute("load parquet")


    def insert(self, rows: List[dict]):
        """
        Creates one or more files in the destination folder based on the partition strategy
        :param rows: Rows of JSON data to be inserted. Must have the expected keys of the partitioning strategy and the sorting order
        """
        partmap = {}
        for row in rows:
            part = self.partStrategy(row)
            if part not in partmap:
                partmap[part] = []
            partmap[part].append(row)

        # for each part map, we can create a file

        # insert into S3
        # insert into meta store
