from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.email import send_email

default_args = {
    'owner': 'prositadmin',
    'start_date': days_ago(0),
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='op1_certbot_renew_and_restart',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['certbot', 'ssl', 'manual', 'op1']
) as dag:

    def check_expiry_and_decide(ti):
        """Check certificate expiry and decide if renewal is needed"""
        print("=== CERTIFICATE EXPIRY CHECK STARTED ===")
        import re
        from datetime import datetime
        import base64
        
        output = ti.xcom_pull(task_ids='check_cert_expiry')
        if not output:
            print("ERROR: No output from certbot certificates command")
            raise ValueError("No output from certbot certificates")
        
        print(f"Raw certbot output received: {len(output)} characters")
        
        try:
            decoded_output = base64.b64decode(output).decode('utf-8')
            print("Successfully decoded base64 output")
        except Exception as e:
            print(f"Base64 decode failed, using raw output: {e}")
            decoded_output = output
        
        print(f"Searching for expiry date in output...")
        # Parse expiry date from certbot output
        match = re.search(r'Expiry Date:\s+([\d-]+\s[\d:]+\+\d{2}:\d{2})', decoded_output)
        if not match:
            print(f"ERROR: Could not find expiry date pattern in output")
            print(f"Certbot output was:\n{decoded_output}")
            raise ValueError(f"Could not find expiry date in certbot output:\n{decoded_output}")
        
        expiry_str = match.group(1)
        print(f"Found expiry date string: {expiry_str}")
        expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d %H:%M:%S%z')
        days_left = (expiry_date - datetime.now(expiry_date.tzinfo)).days
        
        renewal_threshold = 7  # days
        print(f"Certificate expires on: {expiry_date}")
        print(f"Days remaining: {days_left}")
        print(f"Renewal threshold: {renewal_threshold} days")
        
        if days_left <= renewal_threshold:
            print(f" DECISION: Certificate expires in {days_left} days (<= {renewal_threshold}), proceeding with renewal")
            return 'stop_services'
        else:
            print(f" DECISION: Certificate valid for {days_left} more days (> {renewal_threshold}), skipping renewal")
            # Store days_left for use in skip email
            ti.xcom_push(key='days_remaining', value=days_left)
            print(f"Stored {days_left} days remaining in XCom for skip email")
            return 'skip_renewal'

    def decide_copy_restart_needed(ti):
        """Check if certificate was actually renewed and decide if copy/restart is needed"""
        print("=== CHECKING RENEWAL RESULT ===")
        output = ti.xcom_pull(task_ids='renew_certificates')
        if not output:
            print("No output from certificate renewal command")
            print("DECISION: Skipping copy and restart (no renewal output)")
            return 'skip_copy_restart'
        
        print(f"Renewal command output received: {len(output)} characters")
        
        # Check if certbot output indicates renewal occurred
        renewal_indicators = [
            "Successfully renewed",
            "Certificate renewed", 
            "Congratulations! Your certificate",
            "renewed certificate"
        ]
        
        print("Searching for renewal success indicators...")
        for indicator in renewal_indicators:
            if indicator in output:
                print(f"Certificate renewal detected: '{indicator}'")
                print("DECISION: Certificate was renewed, proceeding with copy and restart")
                return 'copy_certs'
        
        print("No certificate renewal indicators found in output")
        print(f"Renewal output was:\n{output[:500]}{'...' if len(output) > 500 else ''}")
        print("DECISION: Skipping copy and restart (no renewal detected)")
        return 'skip_copy_restart'

    def send_skip_email_with_days(ti):
        """Send skip email with actual days remaining"""
        print("=== SENDING SKIP SUCCESS EMAIL ===")
        days_remaining = ti.xcom_pull(task_ids='decide_renewal', key='days_remaining')
        print(f"Retrieved days remaining from XCom: {days_remaining}")
        
        if days_remaining is None:
            print("Warning: Could not retrieve days remaining from XCom, using fallback")
            days_remaining = "more than 7"
        
        subject = 'op1 SSL Certificate Check - No Renewal Needed'
        html_content = f"""
        <h3>SSL certificate check completed successfully - no renewal needed.</h3>
        <ul>
            <li>Host: <code>op1</code></li>
            <li>Certificate is still valid ({days_remaining} days remaining)</li>
            <li>No services were restarted</li>
        </ul>
        """
        
        print(f"Sending email to: mohd.muzamil.08@gmail.com")
        print(f"Subject: {subject}")
        print(f"Days remaining in email: {days_remaining}")
        
        send_email(
            to=['mohd.muzamil.08@gmail.com'],
            subject=subject,
            html_content=html_content
        )
        print(f"Skip email sent successfully with {days_remaining} days remaining")

    # 1. Check certificate expiry status
    check_cert_expiry = SSHOperator(
        task_id='check_cert_expiry',
        ssh_conn_id='ssh_op1',
        command='sudo certbot certificates',
        do_xcom_push=True,
    )

    # 2. Decide if renewal is needed based on expiry
    decide_renewal = BranchPythonOperator(
        task_id='decide_renewal',
        python_callable=check_expiry_and_decide,
    )

    # 3. Stop Docker containers (only if renewal needed)
    stop_services = SSHOperator(
        task_id='stop_services',
        ssh_conn_id='ssh_op1',
        command='cd ~/prosit_universal_build_tools && docker-compose down'
    )

    # 4. Renew certificates
    renew_certificates = SSHOperator(
        task_id='renew_certificates',
        ssh_conn_id='ssh_op1',
        command='sudo certbot renew',
        do_xcom_push=True,
    )

    # 5. Decide if copy and restart is needed based on renewal
    decide_copy_restart = BranchPythonOperator(
        task_id='decide_copy_restart',
        python_callable=decide_copy_restart_needed,
    )

    # 6. Skip renewal (dummy task)
    skip_renewal = DummyOperator(
        task_id='skip_renewal',
    )

    # 7. Copy renewed certs to Docker mount location (only if renewal occurred)
    copy_certs = SSHOperator(
        task_id='copy_certs',
        ssh_conn_id='ssh_op1',
        command=(
            'sudo cp -r /etc/letsencrypt/live/op1.research.cs.dal.ca '
            '/home/sumit/ssl_certs/op1.research.cs.dal.ca'
        ),
    )

    # 8. Change owner of the certs
    change_owner = SSHOperator(
        task_id='change_owner',
        ssh_conn_id='ssh_op1',
        command='sudo chown -R sumit:sumit /home/sumit/ssl_certs/op1.research.cs.dal.ca',
    )

    # 9. Start Docker containers again
    restart_services = SSHOperator(
        task_id='restart_services',
        ssh_conn_id='ssh_op1',
        command='cd ~/prosit_universal_build_tools && docker-compose up -d',
    )

    # 10. Skip copy and restart (dummy task)
    skip_copy_restart = DummyOperator(
        task_id='skip_copy_restart',
    )

    # 11. Send success email for renewal
    send_renewal_success_email = EmailOperator(
        task_id='send_renewal_success_email',
        to='mohd.muzamil.08@gmail.com',
        subject='OP1 SSL Certificate Renewal Successful',
        html_content="""
        <h3>SSL certificate was renewed successfully and services are back online.</h3>
        <ul>
            <li>Host: <code>op1.research.cs.dal.ca</code></li>
            <li>Certs copied to: <code>/home/sumit/ssl_certs/...</code></li>
        </ul>
        """,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    print("Renewal email sent successfully")

    # 12. Send success email for skipped renewal with actual days remaining
    send_skip_success_email = PythonOperator(
        task_id='send_skip_success_email',
        python_callable=send_skip_email_with_days,
    )

    # 13. Send failure alert if anything breaks
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
    print("Failure email sent successfully")

    # DAG task flow
    check_cert_expiry >> decide_renewal
    decide_renewal >> [stop_services, skip_renewal]
    stop_services >> renew_certificates >> decide_copy_restart
    decide_copy_restart >> [copy_certs, skip_copy_restart]
    copy_certs >> change_owner >> restart_services
    restart_services >> [send_renewal_success_email, send_failure_email]
    skip_copy_restart >> [send_renewal_success_email, send_failure_email]
    skip_renewal >> send_skip_success_email
