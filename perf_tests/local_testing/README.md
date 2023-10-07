# Performance Testing

See other files in this directory

## `performance_test.py` 0.7.0

### 100 partitions, 6M rows:

```
============= testing insert performance ==================
making 6000000 fake rows...
made 6000000 rows in 3.094619035720825
Inserted in 8.71054482460022
============= testing insert performance with _partition (no delete) ==================
making 6000000 fake rows...
made 6000000 rows in 4.9547119140625
Inserted in 7.536816835403442
41
============= testing insert performance with _partition ==================
not preserving partitions
making 6000000 fake rows...
made 6000000 rows in 4.905634641647339
Inserted in 7.2159271240234375
```

It seems that somehow doing additional work (deleting the `_partition`) ends up being faster by 10-20%! I've tried 
switching 
the order of the tests too, and I am observing the same results. I'm assuming this is because deleting the 
`_partition` is actually faster than the incurred extra data copy overhead.

### 300 partitions, 6M rows:

```
============= testing insert performance ==================
making 6000000 fake rows...
made 6000000 rows in 3.0531117916107178
Inserted in 10.999592065811157

============= testing insert performance with _partition ==================
making 6000000 fake rows...
made 6000000 rows in 5.250890016555786
Inserted in 9.576282024383545

============= testing insert performance with _partition (no delete) ==================
making 6000000 fake rows...
made 6000000 rows in 5.386840105056763
Inserted in 11.052000045776367
test successful!
```

More partitions results in slower inserts!