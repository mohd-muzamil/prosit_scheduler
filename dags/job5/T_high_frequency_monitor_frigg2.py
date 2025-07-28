# # File: high_frequency_monitor_frigg2.py
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.email_operator import EmailOperator
# from airflow.models import Variable
# import requests

# default_args = {
#     'owner': 'prositadmin',
#     'start_date': datetime(2024, 10, 2),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5)
# }

# dag_high_freq = DAG('high_frequency_monitor_frigg2_server',
#                     description='High-Frequency Monitoring of Frigg2 Server when down',
#                     schedule_interval='*/5 * * * *',
#                     default_args=default_args,
#                     catchup=False)


# def high_frequency_monitoring(**kwargs):
#     server_status = check_server_status()
#     last_status = Variable.get(SERVER_STATUS_VAR, default_var='down')

#     if server_status == 'up' and last_status == 'down':
#         Variable.set(SERVER_STATUS_VAR, server_status)
#         kwargs['ti'].xcom_push(key='send_email_subject', value='Frigg2 Server is Back Online')
#         kwargs['ti'].xcom_push(key='send_email_body', value='Frigg2 Server is Back Online')
#         return True
#     return False


# high_freq_task = PythonOperator(
#     task_id='high_frequency_check',
#     python_callable=high_frequency_monitoring,
#     provide_context=True,
#     dag=dag_high_freq
# )

# send_recovery_email = EmailOperator(
#     task_id='send_recovery_email',
#     to=email_list,
#     subject="{{ ti.xcom_pull(task_ids='high_frequency_check', key='send_email_subject') }}",
#     html_content=""" <h3>Server Recovery Alert:</h3>
#                      <p>{{ ti.xcom_pull(task_ids='high_frequency_check', key='send_email_body') }}</p> """,
#     trigger_rule='one_success',
#     dag=dag_high_freq
# )

# high_freq_task >> send_recovery_email
