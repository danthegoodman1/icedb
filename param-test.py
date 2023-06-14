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

ddb.sql('''
copy (select 1 as t, 'a' as _row_id, 'u_1' as user_id) to 'a.parquet'
''')
ddb.sql('''
copy (select 2 as t, 'a' as _row_id, 'u_1' as user_id) to 'b.parquet'
''')
ddb.sql('''
copy (select 4 as t, 'b' as _row_id, 'u_1' as user_id) to 'c.parquet'
''')
ddb.sql('''
copy (select 3 as t, 'b' as _row_id, 'u_2' as user_id) to 'd.parquet'
''')

print(ddb.sql('''
select _row_id, max(t), argMax(user_id, t)
from read_parquet(['a.parquet', 'b.parquet', 'c.parquet', 'd.parquet'])
group by _row_id
'''))
