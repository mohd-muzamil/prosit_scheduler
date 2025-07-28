job1_op1_mongo_copy.py

2 tasks:  
- Direct copy study_{name} from op1 to study_op1_{name} in loki
- Direct copy from study_op1{name} in mongo to index in loki - index same as the collection name
- Preprocess and upload data into PSQL - same process as older job, iterate over all the collections, segregate data based on attribute and intsert them into subsequent tables in psql.



- Take a look at the script in Job3
Test out and check if the data is being copied into Elastic search, elasticsearch_hook is not tested and code copied directly from chatgpt.
