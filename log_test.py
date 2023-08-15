from icedb.log import IceLogIO, Schema, FileMarker, LogTombstone, S3Client
from time import time

s3c = S3Client(s3prefix="tenant", s3bucket="testbucket", s3region="us-east-1", s3endpoint="http://localhost:9000", s3accesskey="user", s3secretkey="password")

log = IceLogIO("dan-mbp")

# create a log file
sch = Schema()
logFile = log.append(s3c, 1, sch.accumulate(["a", "b"], ["VARCHAR", "BIGINT"]), [FileMarker(
    "tenant/_data/d=2023-08-04/a.parquet", time()-10 * 1000, 123), FileMarker("tenant/_data/d=2023-08-04/b.parquet", time()-11 * 1000, 234), FileMarker("tenant/_data/d=2023-08-04/inevershow.parquet", time()-12 * 1000, 345), FileMarker("tenant/_data/d=2023-08-04/inevershoweither.parquet", time()-12 * 1000, 345)])
print("created log file", logFile)

# pretend inevershow* merged into something
sch = Schema()
logFile = log.append(s3c, 1, sch.accumulate(["a", "b", "c"], ["VARCHAR", "BIGINT", "BIGINT"]), [
    FileMarker("tenant/_data/d=2023-08-04/a.parquet", round(time()-10 * 1000), 123),
    FileMarker("tenant/_data/d=2023-08-04/b.parquet", round(time()-11 * 1000), 234),
    FileMarker("tenant/_data/d=2023-08-05/c.parquet", round(time()-5 * 1000), 123),
    FileMarker("tenant/_data/d=2023-08-05/e.parquet", round(time()-4 * 1000), 234),
    FileMarker("tenant/_data/d=2023-08-04/inevershow.parquet", round(time()-12 * 1000), 345, 1),
    FileMarker("tenant/_data/d=2023-08-04/inevershoweither.parquet", round(time()-12 * 1000), 345, 1)
], [LogTombstone(logFile, round(time()-3 * 1000))])

print("created log file", logFile)

# read in a log file
s1, f1, t1, l1 = log.read_at_max_time(s3c, round(time() * 1000))
print(s1.toJSON())
print(f1)
print(t1)
print(l1)
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
