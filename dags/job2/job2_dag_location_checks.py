from job2 import *

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago

import pandas as pd
import pendulum

# Define default arguments
default_args = {
    'owner': 'prositadmin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Set timezone to Halifax, Canada
local_tz = pendulum.timezone("America/Halifax")
var_config = Variable.get("job1_config", deserialize_json=True)
target_conn_id = var_config["psql_conn_id"]

# Instantiate the DAG
dag = DAG(
    dag_id='job2_locations_outside_canada',
    schedule_interval='0 8 * * *',  # This is a cron expression for 8 AM daily
    catchup=True, tags=['job2', 'locations', 'email', 'source_loki_psql'],
    default_args=default_args,
    description='A simple DAG to check locations outside Canada and send an email',
    start_date=datetime(2024, 7, 18, tzinfo=local_tz),  # Set the start date with timezone
)

def query_postgres(execution_date, **kwargs):
    # Convert execution_date to string in 'YYYY-MM-DD' format
    logical_date = kwargs['logical_date'].strftime('%Y-%m-%d')
    # Calculate the date one day before the logical_date
    date_one_day_before = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    # Connect to Postgres
    postgres_hook = PsqlHook(psql_conn_id=target_conn_id)
    target_conn = postgres_hook.get_conn()
    cursor = target_conn.cursor()

    study = 'study_prositsm'
    
    sql = f"""
            WITH SplitData AS (
            SELECT 
                participantid,
                measuredat,
                uploadedat,
                split_part(location_decrypted, '|', array_length(regexp_split_to_array(location_decrypted, '\|'), 1)) AS address
            FROM {study}.location 
            WHERE location_decrypted NOT LIKE '%Canada%' 
            AND location_decrypted NOT LIKE '%United States%'
            AND location_decrypted NOT LIKE '%None'
            AND DATE(uploadedat) = '{date_one_day_before}'
        )
        SELECT
            participantid,
            DATE(measuredat) AS measured_date,
            DATE(uploadedat) AS uploaded_date,
            address,
            COUNT(*) AS count
        FROM SplitData
        GROUP BY participantid, DATE(measuredat), DATE(uploadedat), address
        ORDER BY measured_date DESC;
    """
    
    cursor.execute(sql)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=colnames)
    print(f"Number of rows: {df.shape[0]}")
    
    # Save the dataframe to an Excel file
    file_name = f'/tmp/participants_outside_canada_{logical_date}.xlsx'
    # save the dataframe to an excel file if it is not empty
    if not df.empty:
        df.to_excel(file_name, index=False)
    
    # Push logical_date and empty flag to XCom
    kwargs['ti'].xcom_push(key='logical_date', value=logical_date)
    kwargs['ti'].xcom_push(key='df_empty', value=df.empty)
    
    cursor.close()
    target_conn.close()

def should_send_email(**kwargs):
    # Pull the empty flag from XCom
    df_empty = kwargs['ti'].xcom_pull(task_ids='run_query', key='df_empty')
    print(f"df_empty: {df_empty}")
    
    if df_empty:
        return 'skip_email'
    else:
        return 'send_email'

def cleanup_file(**kwargs):
    # Get the logical date from XCom
    logical_date = kwargs['ti'].xcom_pull(task_ids='run_query', key='logical_date')
    file_name = f'/tmp/participants_outside_canada_{logical_date}.xlsx'
    
    # Delete the file
    if os.path.exists(file_name):
        os.remove(file_name)
        print(f"Deleted file: {file_name}")
    else:
        print(f"File not found: {file_name}")

# Define the task to run the query
run_query = PythonOperator(
    task_id='run_query',
    python_callable=query_postgres,
    provide_context=True,
    dag=dag,
)

# Define the task to decide if the email should be sent
decision_task = BranchPythonOperator(
    task_id='decision_task',
    python_callable=should_send_email,
    provide_context=True,
    dag=dag,
)

# Define the task to send an email
send_email = EmailOperator(
    task_id='send_email',
    to='mohd.muzamil.08@gmail.com, Nathan.Rowarth@Dal.Ca, prositsm@dal.ca, DawsonSutherland@dal.ca',
    subject="PROSIT Participants Outside Canada_{{ task_instance.xcom_pull(task_ids='run_query', key='logical_date') }}",
    html_content="""<p>Dear team,</p><p>Please find attached the list of participants with addresses outside Canada.</p>""",
    files=["/tmp/participants_outside_canada_{{ task_instance.xcom_pull(task_ids='run_query', key='logical_date') }}.xlsx"],
    dag=dag,
)

# Define a dummy task to skip the email
skip_email = DummyOperator(
    task_id='skip_email',
    dag=dag,
)

# Define the cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_task',
    python_callable=cleanup_file,
    provide_context=True,
    dag=dag,
)
# Set task dependencies
run_query >> decision_task
decision_task >> [send_email, skip_email]
send_email >> cleanup_task