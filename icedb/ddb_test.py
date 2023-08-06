import duckdb
import pandas as pd

ddb = duckdb.connect()

# print(ddb.sql('''
# load httpfs
# '''))
# print(ddb.sql('''
# load parquet
# '''))
# print(ddb.sql('''
# SET s3_region='us-east-1'
# '''))
# print(ddb.sql('''
# SET s3_endpoint='webhook.site'
# '''))
# print(ddb.sql('''
# SET s3_url_style='path'
# '''))
# try:
#   print(ddb.sql('''
#   select * from read_parquet('s3://1d7527f0-be57-4e48-aea1-f988b6ff62f5/ookla-open-data/parquet/performance/type=*/year=*/quarter=*/*.parquet')
#   '''))
# except:
#   pass
# try:
#   print(ddb.sql('''
#   select * from read_parquet('s3://1d7527f0-be57-4e48-aea1-f988b6ff62f5/blah.parquet')
#   '''))
# except:
#   pass

df = pd.DataFrame([{'a': 123, 'b': 1.2, 'c': 'hey', 'd': ['hey']}])
ddb.execute("describe select * from df")
res = ddb.df()
print(res['column_name'].tolist(), res['column_type'].tolist())
print('/'.join([None, 'hey', 'ho']))
