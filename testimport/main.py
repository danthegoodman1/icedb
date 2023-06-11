from icedb import IceDB

def partStrat(row: dict) -> str:
    return row['ts']

ice = IceDB(partitionStrategy=partStrat, sortOrder=['events', 'timestamp'])
