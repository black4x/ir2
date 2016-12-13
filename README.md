# ir
Retrieval System

before run please copy files:
1. documents.zip  ---> prj2/data 
2. tinyir-1.1.jar ---> prj2/lib


run it with the command (15GB to be on the save side):
> sbt -mem 15000 run

then select one of the options:

[1] search.GoQuery  - create index first, then scoring with Language Model and Term-based Model
[2] search.NoIndex  - language model scoring during one run through collection
 
results are saved in files: ranking-[t|l]-28.run