from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime
import os
import subprocess
import zipfile
import shutil

# Function to get MongoDB credentials from Airflow Variables
def get_mongo_credentials(variable_name):
    username = BaseHook.get_connection(variable_name).login
    password = BaseHook.get_connection(variable_name).password
    return username, password

# Function to back up MongoDB (both OP1 and Loki)
def backup_mongo(db_type, db_host, backup_dir, timestamp, op2_backup_dir):
    # Get credentials from Airflow variables
    username, password = get_mongo_credentials(f"{db_type}_mongo_credentials")

    # Connect to MongoDB
    mongo_hook = MongoHook(conn_id=f"{db_type}_mongo")
    db_list = mongo_hook.list_databases()

    # Create backup folder
    backup_folder = f"{backup_dir}/{db_type}_mongodump_{timestamp}"
    os.makedirs(backup_folder, exist_ok=True)

    # Loop through databases and dump each to a zip file
    for db_name in db_list:
        dump_file = f"{backup_folder}/{db_name}.gz"
        dump_cmd = f"mongodump --host {db_host} --username {username} --password {password} --db {db_name} --archive={dump_file} --gzip"
        subprocess.run(dump_cmd, shell=True)

        # Log the document count per collection
        collections = mongo_hook.list_collection_names(db_name)
        for collection in collections:
            collection_count = mongo_hook.count_documents(db_name, collection)
            print(f"Database: {db_name}, Collection: {collection}, Document Count: {collection_count}")

        # Compress into a zip file
        with zipfile.ZipFile(f"{backup_folder}/{db_name}.zip", 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(dump_file, os.path.basename(dump_file))

        # Clean up the temporary dump file
        os.remove(dump_file)

    # Move the backup to OP2 server
    shutil.move(backup_folder, op2_backup_dir)

# Function to back up PostgreSQL (Loki)
def backup_postgres(backup_dir, timestamp, op2_backup_dir):
    # Get PostgreSQL credentials from Airflow variables
    conn_id = "loki_psql_credentials"
    postgres_hook = PostgresHook(postgres_conn_id=conn_id)
    
    # Get a list of databases
    sql = "SELECT datname FROM pg_database WHERE datistemplate = false;"
    databases = postgres_hook.get_records(sql)
    
    # Create backup folder
    backup_folder = f"{backup_dir}/loki_psqldump_{timestamp}"
    os.makedirs(backup_folder, exist_ok=True)

    for db in databases:
        db_name = db[0]
        db_folder = f"{backup_folder}/{db_name}"
        os.makedirs(db_folder, exist_ok=True)

        # Loop through schemas in each database
        sql_schemas = f"SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '{db_name}';"
        schemas = postgres_hook.get_records(sql_schemas)

        for schema in schemas:
            schema_name = schema[0]
            dump_file = f"{db_folder}/{schema_name}.sql"
            dump_cmd = f"pg_dump --host=localhost --username={BaseHook.get_connection(conn_id).login} --password={BaseHook.get_connection(conn_id).password} --dbname={db_name} --schema={schema_name} --file={dump_file}"
            subprocess.run(dump_cmd, shell=True)

            # Log the table sizes
            sql_tables = f"SELECT table_name, pg_total_relation_size(quote_ident(table_name)) AS size FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_catalog = '{db_name}'"
            tables = postgres_hook.get_records(sql_tables)
            for table in tables:
                print(f"Database: {db_name}, Schema: {schema_name}, Table: {table[0]}, Size: {table[1]} bytes")

    # Move the backup to OP2 server
    shutil.move(backup_folder, op2_backup_dir)

# Define the DAG
with DAG(
    'backup_databases',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
    },
    description='Backup MongoDB and PostgreSQL databases',
    schedule_interval='0 20 * * *',  # Run every day at 20:00
    start_date=datetime(2025, 3, 28),
    catchup=False,
) as dag:
    
    # Timestamp for folder naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = "/tmp/backup"  # Set to your desired backup directory
    op2_backup_dir = "/mnt/op2_backup"  # Set to the permanent backup directory on OP2 server
    
    # Task for backing up MongoDB (OP1)
    backup_mongo_op1 = PythonOperator(
        task_id='backup_mongo_op1',
        python_callable=backup_mongo,
        op_args=['op1', 'op1_host', backup_dir, timestamp, op2_backup_dir],
    )

    # Task for backing up MongoDB (Loki)
    backup_mongo_loki = PythonOperator(
        task_id='backup_mongo_loki',
        python_callable=backup_mongo,
        op_args=['loki', 'loki_host', backup_dir, timestamp, op2_backup_dir],
    )

    # Task for backing up PostgreSQL (Loki)
    backup_postgres_loki = PythonOperator(
        task_id='backup_postgres_loki',
        python_callable=backup_postgres,
        op_args=[backup_dir, timestamp, op2_backup_dir],
    )

    # Set task dependencies to run in parallel
    [backup_mongo_op1, backup_mongo_loki, backup_postgres_loki]
