# Dag: job1_mongo_to_mongo
# Description: DAG with tasks to stage mongodb to pslq, copy participant uploads, and backup to external USB disk.
from job1 import *
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
# from airflow.utils.task_group import TaskGroup
from airflow.operators.email_operator import EmailOperator

from job1.job1_task1_mongo_staging import copy_documents_from_mongodb_to_mongodb
from job1.job1_task2_mongo_segregate_studies import segregate_documents_into_study_collections
from job1.job1_task3_elk_staging import copy_documents_from_mongodb_to_elastic_search
from job1.job1_task4_psql_staging import copy_documents_from_mongodb_to_psql


# Getting the configuration from Airflow Variables
'''
job1_config = {
    "source_conn_id": "frigg2_mongo",
    "target_conn_id": "loki_mongo",
    "elk_conn_id": "vidar_elk",
    "psql_conn_id": "loki_psql",
    "source_db_name": "prosit",
    "target_db_name": "entries",
    "psql_staging_db_name": "staging_db",
    "source_collection_name": "entries",
    "target_collection_name": "entries_date",
    "dryrun": True,
    "encryption_key_location": "password1",
    "encryption_key_location_new": "password2"
}
'''
# var_config = job1_config
var_config = Variable.get("job1_config", deserialize_json=True)
sleep_time = 2 * 60     # sleep time in seconds : 2 minute


def get_source_collection_name(date_time: datetime) -> str:
    """
    Get the source collection name based on the execution date.
    source collection name always has end_date_time as per the design.
    args:
        date_time (datetime object) : execution date
    returns:
        source collection name (str): source collection name
    """
    # Extract year, month, and day from the execution date or the end_date_time
    source_conn_id = var_config['source_conn_id']
    source_collection_name = var_config['source_collection_name']
    if source_conn_id == "vidar_mongo":
        # Use below code if data needs to be copied from Vidar instead of Frigg2
        year, month, day = date_time.strftime("%Y-%m-%d").split("-")
        source_date_format = f"{month}_{day}_{year}"
        source_collection_name = source_collection_name.replace("{date}", source_date_format)
    elif source_conn_id == "frigg2_mongo":
        # keeping the source_collection_name as it is.
        source_collection_name = source_collection_name
    else:
        logging.error(f"Invalid source_conn_id: {source_conn_id}. Please provide a valid source_conn_id.")
    return source_collection_name
    

def get_target_collection_name(date_time: datetime) -> str:
    """
    Get the target collection name based on the execution date.
    target collection name always has end_date_time as per the design.
    args:
        date_time (datetime object) : execution date
    returns:
        target collection name (str): target collection name
    """
    # Extract year, month, and day from the execution date or the end_date_time
    year, month, day = date_time.strftime("%Y-%m-%d").split("-")
    target_date_format = f"{year}_{month}_{day}"
    return var_config['target_collection_name'].replace("{date}", target_date_format)


@task()
def mongo_staging(**kwargs):
    """
    Task1: Copy the documents from frigg2 mongodb (operational server) to Loki mongodb (data storage server).
    Builds a custom configuration dictionary and calls the copy_documents_from_mongodb_to_mongodb function.
    args:
        logical_date: execution date
    returns:
        None
    """
    start_date_time = kwargs['logical_date'] - timedelta(minutes=5)
    end_date_time = start_date_time + timedelta(hours=6)
    target_db_name = var_config['target_db_name'] + f"_{end_date_time.year}"
    # Create a new configuration dictionary
    task1_config = {
        'source_conn_id': var_config['source_conn_id'],
        'target_conn_id': var_config['target_conn_id'],
        'source_db_name': var_config['source_db_name'],
        'target_db_name': target_db_name,
        'source_collection_name': get_source_collection_name(start_date_time),
        'target_collection_name': get_target_collection_name(start_date_time),
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'dryrun': var_config['dryrun']
    }
    copy_documents_from_mongodb_to_mongodb(task1_config)
    logging.info(f"Sleeping for {sleep_time} seconds after copying documents from frigg2 mongodb to loki mongodb...")
    time.sleep(sleep_time)
    logging.info(f"Done")
    return None


@task()
def mongo_segregate_studies(**kwargs):
    """
    Task2: Segregate the documents copied from operational server into different studies based on partcipantId.
    Builds a custom configuration dictionary and calls the segregate_documents_into_study_collections function.
    args:
        logical_date: execution date
    returns:
        None
    """
    start_date_time = kwargs['logical_date'] - timedelta(minutes=5)
    end_date_time = start_date_time + timedelta(hours=6)
    target_db_name = var_config['target_db_name'] + f"_{end_date_time.year}"
    task2_config = {
        'conn_id': var_config['target_conn_id'],
        'db_name': target_db_name,
        'collection_name': get_target_collection_name(start_date_time),
        'encryption_key_location': var_config['encryption_key_location'],
        'encryption_key_location_new': var_config['encryption_key_location_new'],
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'dryrun': var_config['dryrun']
    }
    segregate_documents_into_study_collections(task2_config)
    logging.info(f"Sleeping for {sleep_time} seconds after segregating documents into different studies...")
    time.sleep(sleep_time)
    logging.info(f"Done")
    return None

@task()
def elk_staging(**kwargs):
    """
    Task3: Copy the documents from loki mongodb staging_db to vidar Elastic search.
    Builds a custom configuration dictionary and calls the copy_documents_from_mongodb_to_elastic_search function.
    args:
        logical_date: execution date
    returns:
        None
    """
    start_date_time = kwargs['logical_date'] - timedelta(minutes=5)
    end_date_time = start_date_time + timedelta(hours=6)
    # Create a new configuration dictionary
    task3_config = {
        'source_conn_id': var_config['target_conn_id'],     # source is loki mongodb
        'target_conn_id': var_config['elk_conn_id'],        # target is loki Elastic search
        'encryption_key_location_new': var_config['encryption_key_location_new'],
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'dryrun': var_config['dryrun']
    }
    copy_documents_from_mongodb_to_elastic_search(task3_config)
    logging.info(f"Sleeping for {sleep_time} seconds after copying documents from loki mongodb to Loki Elastic search...")
    time.sleep(sleep_time)
    logging.info(f"Done")
    return None


@task()
def psql_staging(**kwargs):
    """
    Task4: Copy the documents from loki mongodb to loki PSQL database.
    Builds a custom configuration dictionary and calls the copy_documents_from_mongodb_to_psql function.
    args:
        logical_date: execution date
    returns:
        None
    """
    start_date_time = kwargs['logical_date'] - timedelta(minutes=5)
    end_date_time = start_date_time + timedelta(hours=6)
    # Create a new configuration dictionary
    task4_config = {
        # source here is loki mongodb, which is the target of task1
        'source_conn_id': var_config['target_conn_id'],
        'target_conn_id': var_config['psql_conn_id'],
        'target_db_name': var_config['psql_staging_db_name'],
        'encryption_key_location_new': var_config['encryption_key_location_new'],
        'start_date_time': start_date_time,
        'end_date_time': end_date_time,
        'dryrun': var_config['dryrun']
    }
    copy_documents_from_mongodb_to_psql(task4_config)
    logging.info(f"Sleeping for {sleep_time} seconds after copying documents from loki mongodb to Loki psql...")
    time.sleep(sleep_time)
    logging.info(f"Done")
    return None


default_args = {
    'owner': 'prositadmin',
    'depends_on_past': False,
    # 'start_date': datetime(2024, 1, 1, 0, 0),        #days_ago(0)
    'start_date': days_ago(3),
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

with DAG(dag_id="job1_mongo_to_mongo", 
         schedule_interval='5 */6 * * *',  # Run every 6 hours
         catchup=True, tags=['job1', 'source_frigg2_mongo', 'target_loki_mongo', 'target_vidar_elk', 'target_loki_psql'],
         description='DAG with tasks to stage mongodb to backup, copy participant uploads, and backup to external USB disk.',
         default_args=default_args) as dag:
    # Task1
    mongo_staging_task = mongo_staging() 
    # Task2
    mongo_segregate_studies_task = mongo_segregate_studies()
    # Task3
    elk_staging_task = elk_staging()
    # Task4
    psql_staging_task = psql_staging()

    # Define the task dependencies
    mongo_staging_task >> mongo_segregate_studies_task
    mongo_segregate_studies_task >> [elk_staging_task, psql_staging_task]
    # [ mongo_staging_task >> mongo_segregate_studies_task >> elk_staging_task >> psql_staging_task ]


