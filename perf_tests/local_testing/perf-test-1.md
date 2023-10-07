From the test, inserting 2000 times with 2 parts, shows performance against S3 and reading in the state and schema

```
============== insert hundreds ==============
this will take a while...
inserted 200
inserted hundreds in 11.283345699310303
reading in the state
read hundreds in 0.6294591426849365
files 405 logs 202
verify expected results
got 405 alive files
[(406, 'a'), (203, 'b')] in 0.638556957244873
merging it
merged partition cust=test/d=2023-02-11 with 203 files in 1.7919442653656006
read post merge state in 0.5759727954864502
files 406 logs 203
verify expected results
got 203 alive files
[(406, 'a'), (203, 'b')] in 0.5450308322906494
merging many more times to verify
merged partition cust=test/d=2023-06-07 with 200 files in 2.138633966445923
merged partition cust=test/d=2023-06-07 with 3 files in 0.638775110244751
merged partition None with 0 files in 0.5988118648529053
merged partition None with 0 files in 0.6049611568450928
read post merge state in 0.6064021587371826
files 408 logs 205
verify expected results
got 2 alive files
[(406, 'a'), (203, 'b')] in 0.0173952579498291
tombstone clean it
tombstone cleaned 4 cleaned log files, 811 deleted log files, 1012 data files in 4.3332929611206055
read post tombstone clean state in 0.0069119930267333984
verify expected results
got 2 alive files
[(406, 'a'), (203, 'b')] in 0.015745878219604492

============== insert thousands ==============
this will take a while...
inserted 2000
inserted thousands in 107.14211988449097
reading in the state
read thousands in 7.370793104171753
files 4005 logs 2002
verify expected results
[(4006, 'a'), (2003, 'b')] in 6.49034309387207
merging it
breaking on marker count
merged 2000 in 16.016802072525024
read post merge state in 6.011193037033081
files 4006 logs 2003
verify expected results
[(4006, 'a'), (2003, 'b')] in 6.683710098266602
# laptop became unstable around here
```

Some notes:

1. Very impressive state read performance with so many files (remember it has to open each one and accumulate the
   state!)
2. Merging happens very quick
3. Tombstone cleaning happens super quick as well
4. DuckDB performs surprisingly well with so many files (albeit they are one or two rows each)
5. At hundreds of log files and partitions (where most tables should live at), performance was exceptional
6. Going from hundreds to thousands, performance is nearly perfectly linear, sometimes even super linear (merges)!

Having such a large log files (merged but not tombstone cleaned) is very unrealistic. Chances are worst case you
have <100 log files and hundreds or low thousands of data files. Otherwise you are either not merging/cleaning
enough, or your partition scheme is far too granular.

The stability of my laptop struggled when doing the thousands test, so I only showed where I could consistently get to.