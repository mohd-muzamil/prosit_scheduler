# dags/mongo_to_elk_historical_dag.py
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime
from job7_mongo_to_elk_historical_load.mongo_elk_loader import copy_documents_from_mongodb_to_elastic_search

var_config = Variable.get("mongo_to_elk_historical_config", deserialize_json=True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 1),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='mongo_to_elk_historical_loader',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='One-time DAG to backfill MongoDB to ELK for a historical range',
    tags=['mongo', 'elk', 'historical']
) as dag:

    def run_historical_load():
        config = {
            'source_conn_id': var_config['loki_mongo_conn_id'],
            'target_conn_id': var_config['vidar_elk_conn_id'],
            'encryption_keys': [
                var_config['encryption_key_location_new'],
                var_config['encryption_key_location_old']
            ],
            'secret_key': var_config['secret_key'],
            'dryrun': var_config.get('dryrun', True),
            'batch_size': int(var_config.get('batch_size', 10000)),
            'max_workers': int(var_config.get('max_workers', 4)),
            'allowed_dbs': var_config.get('allowed_dbs', [])
        }
        # Optional time range (uncomment if needed)
        # config['start_date_time'] = datetime(2024, 8, 10)
        # config['end_date_time'] = datetime(2024, 8, 20)
        return copy_documents_from_mongodb_to_elastic_search(config)

    load_task = PythonOperator(
        task_id='load_mongo_data_to_elk',
        python_callable=run_historical_load
    )

    load_task
