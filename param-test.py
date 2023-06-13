import duckdb

ddb = duckdb.connect()

ddb.sql('''
copy (select 1 as a, 'c' as b) to 'a.parquet'
''')
ddb.sql('''
copy (select 1 as a, 'a' as b) to 'b.parquet'
''')

ddb.sql('''
copy (select 1 as a, 'a' as b) to 'c.parquet'
''')

print(ddb.execute('''
select * from read_parquet(?)
''', [['a.parquet', 'b.parquet']]).df().to_csv(index=False))

print(ddb.execute('''
select sum(a), gen_random_uuid()::TEXT, b from read_parquet(?) group by b
''', [['a.parquet', 'b.parquet', 'c.parquet']]).df().to_csv(index=False))

print(ddb.sql('''
copy (
select * from read_parquet('a.parquet')
) to 'd.parquet'
'''))
