from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from job9_op1_audio_daily_pipeline.job9_task1_download_op1_audio_files import download_audio_files
from job9_op1_audio_daily_pipeline.job9_task2_decrypt_convert_acc_to_wav import process_files
from job9_op1_audio_daily_pipeline.job9_task3_prepare_vidar_wav_sync import prepare_files


default_args = {
    'owner': 'prositadmin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='audio_pipeline_op1_to_vidar',
    default_args=default_args,
    description='Download, decrypt, convert, and sync audio files to Vidar',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)


download_audio = PythonOperator(
    task_id='download_audio_files',
    python_callable=download_audio_files,
    dag=dag,
)

decrypt_convert_audio = PythonOperator(
    task_id='decrypt_and_convert_to_wav',
    python_callable=process_files,
    dag=dag,
)


prepare_sync_files = PythonOperator(
    task_id='prepare_wav_files_for_vidar',
    python_callable=prepare_files,
    dag=dag,
)

sync_to_vidar = BashOperator(
    task_id='sync_to_vidar_and_set_permissions',
    bash_command="""
    rsync -avz /opt/airflow/dags/job9_op1_audio_daily_pipeline/vidar_sync/op1_audio_files/ \
    nathan@vidar.research.cs.dal.ca:/home/nathan/op1_audio_files/ && \
    ssh nathan@vidar.research.cs.dal.ca 'chown -R nathan:nathan /home/nathan/op1_audio_files/ && chmod -R 700 /home/nathan/op1_audio_files/'
    """,
    dag=dag,
)

download_audio >> decrypt_convert_audio >> prepare_sync_files >> sync_to_vidar
