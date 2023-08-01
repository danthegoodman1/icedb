select * FROM s3('https://webhook.site/1d7527f0-be57-4e48-aea1-f988b6ff62f5/ookla-open-data/parquet/performance/type=*/year=*/quarter=*/*.parquet', 'Parquet', 'quadkey Nullable(String), tile Nullable(String), avg_d_kbps Nullable(Int64), avg_u_kbps Nullable(Int64), avg_lat_ms Nullable(Int64), tests Nullable(Int64), devices Nullable(Int64)')


select * FROM s3('https://webhook.site/1d7527f0-be57-4e48-aea1-f988b6ff62f5/ookla-open-data/parquet/performance/a.parquet', 'Parquet', 'quadkey Nullable(String), tile Nullable(String), avg_d_kbps Nullable(Int64), avg_u_kbps Nullable(Int64), avg_lat_ms Nullable(Int64), tests Nullable(Int64), devices Nullable(Int64)')
