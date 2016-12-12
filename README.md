# ir
Retrieval System

before run please copy file documents.zip into: prj2/data 


run it with the command:
> sbt -mem 4096 run

then select one of the options:

[1] search.LazyIndex  - create index first, then scoring with Language Model and Token Model
[2] search.NoIndex    - language model scoring during one run through collection
 
results are saved in file: XXX