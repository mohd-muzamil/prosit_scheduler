from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers

# MongoDB and Elasticsearch configuration
MONGO_URI = 'mongodb://root:sierra@op1.research.cs.dal.ca:27016/admin?authSource=admin'
ELASTICSEARCH_HOST = 'localhost'
ELASTICSEARCH_PORT = 9200

def copy_mongo_to_elasticsearch(**kwargs):
    mongo_client = MongoClient(MONGO_URI)
    es = Elasticsearch([{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}])

    # List of databases to iterate over
    db_names = ['database1', 'database2']  # Replace with your MongoDB database names

    for db_name in db_names:
        mongo_db = mongo_client[db_name]
        
        # Iterate over collections in the database
        collection_names = mongo_db.list_collection_names()
        
        for collection_name in collection_names:
            mongo_collection = mongo_db[collection_name]
            es_index_name = f'{db_name}_{collection_name}'

            # Fetch data from MongoDB
            documents = mongo_collection.find()
            actions = []

            for doc in documents:
                # Prepare data for Elasticsearch
                action = {
                    "_index": es_index_name,
                    "_source": doc
                }
                actions.append(action)

            if actions:
                # Bulk insert into Elasticsearch
                helpers.bulk(es, actions)

    mongo_client.close()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'mongo_to_elasticsearch',
    default_args=default_args,
    description='Copy data from MongoDB to Elasticsearch',
    schedule_interval=None,  # Set to your desired schedule or use cron expressions
)

# Define the task
copy_task = PythonOperator(
    task_id='copy_mongo_to_elasticsearch',
    python_callable=copy_mongo_to_elasticsearch,
    provide_context=True,
    dag=dag,
)

copy_task
