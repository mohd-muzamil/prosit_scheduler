from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'prositadmin',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='certbot_renew_and_restart',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['certbot', 'ssl', 'manual'],
) as dag:

    # 1. Stop Docker containers
    stop_services = SSHOperator(
        task_id='stop_services',
        ssh_conn_id='ssh_op1',
        command='cd ~/prosit_universal_build_tools && docker-compose down'
    )


    # 2. Renew certificates
    renew_certificates = SSHOperator(
        task_id='renew_certificates',
        ssh_conn_id='ssh_op1',
        command='sudo certbot renew',
    )

    # 3. Copy renewed certs to Docker mount location
    copy_certs = SSHOperator(
        task_id='copy_certs',
        ssh_conn_id='ssh_op1',
        command=(
            'sudo cp -r /etc/letsencrypt/live/op1.research.cs.dal.ca '
            '/home/sumit/ssl_certs/op1.research.cs.dal.ca'
        ),
    )

    # 4. Change owner of the certs
    change_owner = SSHOperator(
        task_id='change_owner',
        ssh_conn_id='ssh_op1',
        command='sudo chown -R sumit:sumit /home/sumit/ssl_certs/op1.research.cs.dal.ca',
    )

    # 5. Start Docker containers again
    restart_services = SSHOperator(
        task_id='restart_services',
        ssh_conn_id='ssh_op1',
        command='cd ~/prosit_universal_build_tools && docker-compose up -d',
    )

    # 6. Send success email
    send_success_email = EmailOperator(
        task_id='send_success_email',
        to='mohd.muzamil.08@gmail.com',
        subject='SSL Certificate Renewal Successful',
        html_content="""
        <h3>SSL certificate was renewed successfully and services are back online.</h3>
        <ul>
            <li>Host: <code>op1.research.cs.dal.ca</code></li>
            <li>Certs copied to: <code>/home/sumit/ssl_certs/...</code></li>
        </ul>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # 7. Send failure alert if anything breaks
    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to='mohd.muzamil.08@gmail.com',
        subject='SSL Certificate Renewal FAILED',
        html_content="""
        <h3>SSL Certificate renewal or restart failed on <code>op1.research.cs.dal.ca</code>.</h3>
        <p>Check logs and the system immediately.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # DAG task flow
    (
        stop_services
        >> renew_certificates
        >> copy_certs
        >> change_owner
        >> restart_services
        >> [send_success_email, send_failure_email]
    )
