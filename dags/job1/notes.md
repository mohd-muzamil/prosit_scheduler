# job1_dag_mongo_copy.py  - Task Descriptions

## Overview
This document provides an overview of the tasks involved in dag `job1_dag_mongo_copy.py` of the Prosit Scheduler.
This dag copies the data from Frigg2 mongodb to Loki mongodb and then to loki postgreSQL.
Dag is designed to run every 6hours at 00:05hrs, 06:05hrs, 12:05hrs, 18:05hrs

## Tasks
job1_task1_mongo_staging.py - copy data from frigg2 mongoodb to loki mongodb 
job1_task2_mongo_segregate_studies.py - segregarte data in loki mongodb into staging_db and different study_db based on participanId
job1_task3_psql_staging.py - copy the data from staging_db into PSQL
