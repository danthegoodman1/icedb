# IceDB

## Optimization Opportunities

1. Streaming Mutations
2. Streaming inserts - break out the endpoints for different body formats and process rows one by one instead of loading entirely into memory every time
3. Strong concurrent merge protection - rather than holding a transaction open we are relying on timeouts right now. A better way would be to use a lock within the DB that is just for merging, and lists files that are under merge. It should have a timeout as well but allows for concurrent merges because a subsequent concurrent merge can ignore those files.
4. Concurrent merge reading - reading the files to merge concurrently instead of sequentially
