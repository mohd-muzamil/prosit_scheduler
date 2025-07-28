from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

def print_hello():
    return 'Hello world!'

default_args = {
        'owner': 'peter',
        'start_date':datetime(2018,8,11),
}

dag = DAG('send_test_email', description='Simple tutorial DAG',
          schedule_interval=None,
          default_args = default_args, catchup=False)


# hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

email = EmailOperator(
        task_id='send_email',
        to='mohd.muzamil.08@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """,
        dag=dag
)

# print the location of airflow.cfg
import os
print(os.environ['AIRFLOW_HOME'])

# hello_operator >> email
email

