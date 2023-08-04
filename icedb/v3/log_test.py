from log import IceLogIO, Schema, FileMarker, FileTombstone, S3Client
from time import time

s3c = S3Client(s3prefix="tenant/_log", s3bucket="testbucket", s3region="us-east-1", s3endpoint="http://localhost:9000", s3accesskey="user", s3secretkey="password")

log = IceLogIO()

# create a log file
logFile = log.append(s3c, 1, Schema(["a", "b"], ["VARCHAR", "BIGINT"]), [FileMarker("tenant/_data/d=2023-08-04/c.parquet", time()-10 * 1000, 123, "tenant/_data/d=2023-08-04/a.parquet"), FileMarker("tenant/_data/d=2023-08-04/d.parquet", time()-11 * 1000, 234)], [FileTombstone("tenant/_data/d=2023-08-04/a.parquet", time()-13 * 1000), FileTombstone("tenant/_data/d=2023-08-04/b.parquet", time()-12 * 1000)])
print("created log file", logFile)

logFile = log.append(s3c, 1, Schema(["a", "b", "c"], ["VARCHAR", "BIGINT", "BIGINT"]), [FileMarker("tenant/_data/d=2023-08-05/e.parquet", time()-5 * 1000, 123, "tenant/_data/d=2023-08-05/f.parquet"), FileMarker("tenant/_data/d=2023-08-05/g.parquet", time()-4 * 1000, 234)])
print("created log file", logFile)

# read in a log file
s1, f1, t1 = log.readAtMaxTime(s3c, time()*1000)
# s2, f2, t2 = log.readAtMaxTime(s3c, time()-9*1000)
