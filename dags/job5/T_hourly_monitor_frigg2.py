from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import subprocess

default_args = {
    'owner': 'prositadmin',
    'start_date': datetime(2024, 10, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('hourly_monitor_frigg2_server',
          description='Monitor Frigg2 Server and trigger high-frequency checks if needed',
          schedule_interval='@hourly',
          default_args=default_args,
          catchup=False)

SERVER_STATUS_VAR = 'server_status'
SERVER_URL = 'http://frigg2.research.cs.dal.ca/api/rsakey'


def check_server_status(**kwargs):
    try:
        response = requests.get(SERVER_URL, timeout=10)
        if response.status_code == 200:
            server_status = 'up'
        else:
            server_status = 'down'
    except requests.exceptions.RequestException:
        server_status = 'down'

    last_status = Variable.get(SERVER_STATUS_VAR, default_var='up')
    if server_status != last_status:
        Variable.set(SERVER_STATUS_VAR, server_status)
        kwargs['ti'].xcom_push(key='send_email_subject', value=f"Frigg2 Server is {server_status.capitalize()}")
        kwargs['ti'].xcom_push(key='send_email_body', value=f"Frigg2 Server status changed to: {server_status.capitalize()}")
        return True if server_status == 'down' else False
    return False


monitor_task = PythonOperator(
    task_id='check_server_status',
    python_callable=check_server_status,
    provide_context=True,
    dag=dag
)

# Trigger the high-frequency monitoring DAG if the server is down
trigger_high_freq = TriggerDagRunOperator(
    task_id='trigger_high_freq_monitoring',
    trigger_dag_id='high_frequency_monitor_frigg2_server',
    execution_date='{{ execution_date }}',
    wait_for_completion=False,
    dag=dag
)

# Email alert if the server status changed
email_list = ['mohd.muzamil.08@gmail.com', 'sandra.meier@iwk.nshealth.ca', 'Nathan.Rowarth@Dal.Ca', 'DawsonSutherland@dal.ca', 'sm585558@dal.ca']
email_list = email_list[0]  # For testing purposes

send_email = EmailOperator(
    task_id='send_email',
    to=email_list,
    subject="{{ ti.xcom_pull(task_ids='check_server_status', key='send_email_subject') }}",
    html_content=""" <h3>Server Monitoring Alert:</h3>
                     <p>{{ ti.xcom_pull(task_ids='check_server_status', key='send_email_body') }}</p> """,
    trigger_rule='one_success',
    dag=dag
)

monitor_task >> send_email
monitor_task >> trigger_high_freq
