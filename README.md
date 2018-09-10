Problem: Time Series Merge
==========================
Time series are stored in files with the following format:
● files are multiline plain text files in ASCII encoding
● each line contains exactly one record
● each record contains date and integer value; records are encoded like so: yyyy-MM-dd:X
● dates within single file are non-duplicate and sorted in ascending order ● files can be bigger than RAM available on target host

The result program will merge arbitrary number of files, up to 100, into one file. 
Result file follows the same format conventions as described above. Records with same date value are merged into one by summing up X values.

###Build and run

`sbt assembly`
`java -jar target/scala-2.12/aggregator.jar -d test/ -o agg.txt` or `java -jar target/scala-2.12/aggregator.jar -f test/f1.txt,test/f2.txt -o agg.txt`
```
$ cat agg.txt
 2018-01-01:3
 2018-01-02:2
 2018-01-03:3
 2018-01-04:1
 2018-01-05:5
 2018-01-06:4
 2018-01-07:2
 2018-01-08:1
 2018-01-10:1
```

###Help

```
usage: Aggregator [-d <arg>] [-f <arg>] [-h] -o <arg>
Time series aggregator
 -d,--src-dir <arg>       directory with source files, if not defined
                          option 'f' must exist
 -f,--src-files <arg>     individual comma separated source files, if not
                          defined option 'd' must exist
 -h,--help                Prints usage
 -o,--output-file <arg>   output file
```
