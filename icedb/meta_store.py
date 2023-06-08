from typing import List
class MetaStore:
    def insert_partition(self, partition: List[List[str]], file_name: str):
        """
        Inserts a partition into the meta store
        :param partition: ordered pair of partition keys and values, like [["year", "2023], ["month", "06"]]
        :param file_name: generated file name (not including path), such as aaa-bbb-ccc.parquet
        """
        pass