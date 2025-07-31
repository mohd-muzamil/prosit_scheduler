from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
import re
from datetime import datetime, timedelta
import base64

default_args = {
    'owner': 'prositadmin',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id='op1_certbot_expiry_monitor',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['certbot', 'monitoring', 'op1'],
)


def parse_certbot_output(ti):
    output = ti.xcom_pull(task_ids='get_certbot_certificates')

    if not output:
        raise ValueError("No output from certbot certificates")

    # Decode base64 if needed
    try:
        decoded_output = base64.b64decode(output).decode('utf-8')
    except Exception:
        # If decode fails, maybe output is already plain text
        decoded_output = output

    # Use regex on decoded output
    match = re.search(r'Expiry Date:\s+([\d-]+\s[\d:]+\+\d{2}:\d{2})', decoded_output)
    if not match:
        raise ValueError(f"Could not find expiry date in certbot output:\n{decoded_output}")

    expiry_str = match.group(1)
    expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d %H:%M:%S%z')

    days_left = (expiry_date - datetime.now(expiry_date.tzinfo)).days
    alert_threshold = 7
    email_list = ['mohd.muzamil.08@gmail.com']

    if days_left < alert_threshold:
        subject = f"[ALERT] OP1 SSL Certificate expiring in {days_left} days"
        body = f"The SSL certificate for op1.research.cs.dal.ca expires on {expiry_date}.\n\nCertbot output:\n\n{decoded_output}"
        send_email(to=email_list, subject=subject, html_content=body)
        print(f"Alert sent: certificate expires in {days_left} days")
    else:
        print(f"Certificate valid for {days_left} more days, no alert sent.")


get_certbot_certificates = SSHOperator(
    task_id='get_certbot_certificates',
    ssh_conn_id='ssh_op1',
    command='sudo certbot certificates',
    do_xcom_push=True,
    dag=dag,
)

check_cert_expiry = PythonOperator(
    task_id='check_cert_expiry',
    python_callable=parse_certbot_output,
    dag=dag,
)

get_certbot_certificates >> check_cert_expiry
