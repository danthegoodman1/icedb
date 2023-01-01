# IceDB

A cloud-native JSON-first serverless analytical database designed for low cost and arbitrary read capacity while maintaining the benefits of columnar data formats. It leverages object storage, serverless compute, and serverless databases to provide an infinitely scalable data warehouse for JSON data in which you only pay for storage when idle. It is called “IceDB” because it is designed for cold-starts: Going from idle to high-performance querying in milliseconds.

https://blog.danthegoodman.com/introducing-icedb--a-serverless-json-analytics-database

## Optimization Opportunities

1. Streaming Mutations
2. Streaming inserts - break out the endpoints for different body formats and process rows one by one instead of loading entirely into memory every time
3. Strong concurrent merge protection - rather than holding a transaction open we are relying on timeouts right now. A better way would be to use a lock within the DB that is just for merging, and lists files that are under merge. It should have a timeout as well but allows for concurrent merges because a subsequent concurrent merge can ignore those files.
4. Concurrent merge reading - reading the files to merge concurrently instead of sequentially
