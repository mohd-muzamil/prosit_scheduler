# Dag: job1_mongo_to_mongo
# Description: DAG with tasks to stage mongodb to pslq, copy participant uploads, and backup to external USB disk.
from job3 import *
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
# from airflow.utils.task_group import TaskGroup
from airflow.operators.email_operator import EmailOperator

from job3.job3_task1_mongo_staging import copy_documents_from_mongodb_to_mongodb
from job3.job3_task2_elk_staging import copy_documents_from_mongodb_to_elastic_search
from job3.job3_task3_psql_staging import copy_documents_from_mongodb_to_psql


# Getting the configuration from Airflow Variables
'''
job3_config = {
    "source_conn_id": "op1_mongo",
    "target_conn_id": "loki_mongo",
    "elk_conn_id": "vidar_elk",
    "psql_conn_id": "loki_psql",
    "psql_staging_db_name": "staging_db",
    "dryrun": True,
    "encryption_key": "password1",
}
'''
# var_config = job1_config
var_config = Variable.get("job3_config", deserialize_json=True)
sleep_time = 2 * 0     # sleep time in seconds : 2 minute

# rename this task
@task()
def mongo_staging(**kwargs):
    """
    Task1: Copy the documents from op1 mongodb to loki mongodb.
    Builds a custom configuration dictionary and calls the copy_documents_from_mongodb_to_mongodb function.
    args:
        logical_date: execution date
    returns:
        None
    """
    start_date_time = kwargs['logical_date'] - timedelta(minutes=15) # negative timedelta of 5mins is added because task is scheduled to run at 5th minute of the hour
    end_date_time = start_date_time + timedelta(hours=6)    # 6 hours of data is copied

    task1_config = {
        'source_conn_id': var_config['source_conn_id'],
        'target_conn_id': var_config['target_conn_id'],
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'dryrun': var_config['dryrun']
    }
    logging.info("task1_config: ", task1_config)
    copy_documents_from_mongodb_to_mongodb(task1_config)
    logging.info(f"Sleeping for {sleep_time} seconds after segregating documents into different studies...")
    time.sleep(sleep_time)
    logging.info(f"Done")
    return None

# add another task to stage data from loki mongodb staging_db to loki Elastic search.
@task()
def elk_staging(**kwargs):
    """
    Task2: Copy the documents from loki mongodb staging_db to vidar Elastic search.
    Builds a custom configuration dictionary and calls the copy_documents_from_mongodb_to_elastic_search function.
    args:
        logical_date: execution date
    returns:
        None
    """
    start_date_time = kwargs['logical_date'] - timedelta(minutes=15)
    end_date_time = start_date_time + timedelta(hours=6)
    # Create a new configuration dictionary
    task2_config = {
        'source_conn_id': var_config['target_conn_id'],     # source is loki mongodb
        'target_conn_id': var_config['elk_conn_id'],        # target is loki Elastic search
        'encryption_key_location': var_config['encryption_key_location_old'],
        'secret_key': var_config['secret_key'],
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'dryrun': var_config['dryrun']
    }
    copy_documents_from_mongodb_to_elastic_search(task2_config)
    logging.info(f"Sleeping for {sleep_time} seconds after copying documents from loki mongodb to Loki Elastic search...")
    time.sleep(sleep_time)
    logging.info(f"Done")
    return None


# copy the data from loki staging_db to loki psql - some pre-processing is done before copying the data to psql
@task()
def psql_staging(**kwargs):
    """
    Task3: Copy the documents from loki mongodb to loki PSQL database.
    Builds a custom configuration dictionary and calls the copy_documents_from_mongodb_to_psql function.
    args:
        logical_date: execution date
    returns:
        None
    """
    start_date_time = kwargs['logical_date'] - timedelta(minutes=15)
    end_date_time = start_date_time + timedelta(hours=6)   #change this to 6 hours later
    # Create a new configuration dictionary
    task3_config = {
        # source here is loki mongodb, which is the target of task1
        'source_conn_id': var_config['target_conn_id'],
        'target_conn_id': var_config['psql_conn_id'],
        'target_db_name': var_config['psql_staging_db_name'],
        'encryption_key_location': var_config['encryption_key_location_old'],
        'secret_key': var_config['secret_key'],
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'dryrun': var_config['dryrun']
    }
    copy_documents_from_mongodb_to_psql(task3_config)
    logging.info(f"Sleeping for {sleep_time} seconds after copying documents from loki mongodb to Loki psql...")
    time.sleep(sleep_time)
    logging.info(f"Done")
    return None


default_args = {
    'owner': 'prositadmin',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 1, 0, 0),        #days_ago(0)
    # 'start_date': days_ago(13),
    # 'end_date': datetime(2024, 1, 1, 2, 0),  # end date is not required as we are using schedule_interval, data needs to be copied for every hour forever...
    'email_on_failure': True,
    'email_on_retry': True,
    'email': 'mohd.muzamil.08@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'max_active_runs': 1,  # Set the maximum number of active DAG runs at a time
    'concurrency': 1,
    'dag_run_timeout': timedelta(minutes=30),  # Set a timeout between runs to avoid stressing the database when backfilling
}

with DAG(dag_id="job3_op1_mongo_copy", 
         schedule_interval='15 */6 * * *',  # Run every 6 hours past the 15th minute of the hour
        #  run every 30 minutes
        # schedule_interval='*/30 * * * *',  # Run every 30 minutes
        # schedule_interval='5 0 * * *',  # Run every day at 00:05 AM
         catchup=True, tags=['job3', 'source_op1_mongo', 'target_loki_mongo', 'target_vidar_elk', 'target_loki_psql'],
         description='DAG with tasks to stage mongodb to backup, copy participant uploads, and backup to external USB disk.',
         default_args=default_args) as dag:
    
    # Task1
    mongo_staging_task = mongo_staging()
    # Task2
    elk_staging_task = elk_staging()
    # Task3
    psql_staging_task = psql_staging()
    
    # lets trigger mongo_staging_task first fllowed by elk_staging_task and then psql_staging_task in parallel
    mongo_staging_task >> [elk_staging_task, psql_staging_task]

