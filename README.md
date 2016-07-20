# TennBase
a lightweight datastore based on B*tree implements some basic function  of hbase.

thread-safe, simple transaction, reach by b-start tree, append new data to data file in order to insert and update quickly. 

consist of three parts, b-start tree index, data file and logs.
