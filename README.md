# ir
Retrieval System

before run please copy files:
1. documents.zip  ---> prj2/data 
2. tinyir-1.1.jar ---> prj2/lib


run it with the command:
> sbt -mem 4096 run

then select one of the options:

[1] search.LazyIndex  - create index first, then scoring with Language Model and Token Model
[2] search.NoIndex    - language model scoring during one run through collection
 
results are saved in file: XXX