from log import IceLogIO, Schema, FileMarker, LogTombstone, S3Client
from time import time

s3c = S3Client(s3prefix="tenant/_log", s3bucket="testbucket", s3region="us-east-1", s3endpoint="http://localhost:9000", s3accesskey="user", s3secretkey="password")

log = IceLogIO()

# create a log file
logFile = log.append(s3c, 1, Schema().accumulate(["a", "b"], ["VARCHAR", "BIGINT"]), [FileMarker("tenant/_data/d=2023-08-04/a.parquet", time()-10 * 1000, 123), FileMarker("tenant/_data/d=2023-08-04/b.parquet", time()-11 * 1000, 234), FileMarker("tenant/_data/d=2023-08-04/inevershow.parquet", time()-12 * 1000, 345)])
print("created log file", logFile)
logFile = log.append(s3c, 1, Schema().accumulate(["a", "b", "c"], ["VARCHAR", "BIGINT", "BIGINT"]), [FileMarker("tenant/_data/d=2023-08-05/c.parquet", time()-5 * 1000, 123, "tenant/_data/d=2023-08-05/d.parquet"), FileMarker("tenant/_data/d=2023-08-05/e.parquet", time()-4 * 1000, 234), FileMarker("tenant/_data/d=2023-08-04/inevershow.parquet", time()-12 * 1000, 345, logFile)], [LogTombstone(logFile, time()-3 * 1000)])
print("created log file", logFile)

# read in a log file
s1, f1, t1 = log.readAtMaxTime(s3c, time()*1000)
print(s1.toJSON())
print(list(map(lambda x: x.path, f1)))
print(list(map(lambda x: x.path, t1)) if t1 != None else None)
# s2, f2, t2 = log.readAtMaxTime(s3c, time()-9*1000)

logFiles = s3c.s3.list_objects_v2(
    Bucket=s3c.s3bucket,
    MaxKeys=1000,
    Prefix=s3c.s3prefix
)
for file in logFiles['Contents']:
    key = file['Key']
    print('deleting', key)
    s3c.s3.delete_object(
        Bucket=s3c.s3bucket,
        Key=key
    )
