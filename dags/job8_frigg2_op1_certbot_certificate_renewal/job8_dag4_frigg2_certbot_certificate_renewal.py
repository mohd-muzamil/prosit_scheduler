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
    dag_id='frigg2_certbot_renew_and_restart',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['certbot', 'ssl', 'manual', 'frigg2'],
) as dag:

    def check_expiry_and_decide(ti):
        """Check certificate expiry and decide if renewal is needed"""
        print("=== FRIGG2 CERTIFICATE EXPIRY CHECK STARTED ===")
        import re
        from datetime import datetime
        import base64
        
        output = ti.xcom_pull(task_ids='check_cert_expiry')
        if not output:
            print("ERROR: No output from certbot certificates command")
            raise ValueError("No output from certbot certificates")
        
        print(f"Raw certbot output received: {len(output)} characters")
        
        # Decode base64 if needed
        try:
            decoded_output = base64.b64decode(output).decode('utf-8')
            print("Successfully decoded base64 output")
        except Exception as e:
            print(f"Base64 decode failed, using raw output: {e}")
            decoded_output = output
        
        print(f"Searching for expiry date in output...")
        # Parse expiry date from certbot certificates output
        match = re.search(r'Expiry Date:\s+([\d-]+\s[\d:]+\+\d{2}:\d{2})', decoded_output)
        if not match:
            print(f"ERROR: Could not find expiry date pattern in output")
            print(f"Certbot output was:\n{decoded_output}")
            raise ValueError(f"Could not find expiry date in certbot output:\n{decoded_output}")
        
        expiry_str = match.group(1)
        print(f"Found expiry date string: {expiry_str}")
        expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d %H:%M:%S%z')
        days_left = (expiry_date - datetime.now(expiry_date.tzinfo)).days
        
        # Only renew if certificate expires within 7 days
        renewal_threshold = 7
        print(f"Certificate expires on: {expiry_date}")
        print(f"Days remaining: {days_left}")
        print(f"Renewal threshold: {renewal_threshold} days")
        
        if days_left <= renewal_threshold:
            print(f" DECISION: Certificate expires in {days_left} days (<= {renewal_threshold}), proceeding with renewal")
            return 'renew_certificates'
        else:
            print(f" DECISION: Certificate valid for {days_left} more days (> {renewal_threshold}), skipping renewal")
            # Store days_left for use in skip email
            ti.xcom_push(key='days_remaining', value=days_left)
            print(f"Stored {days_left} days remaining in XCom for skip email")
            return 'skip_renewal'

    def decide_restart_needed(ti):
        """Check if certificate was actually renewed and decide if restart is needed"""
        print("=== CHECKING FRIGG2 RENEWAL RESULT ===")
        output = ti.xcom_pull(task_ids='renew_certificates')
        if not output:
            print("❌ No output from certificate renewal command")
            print("DECISION: Skipping nginx restart (no renewal output)")
            return 'skip_restart'
        
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
                print("DECISION: Certificate was renewed, restarting nginx container")
                return 'restart_nginx_container'
        
        print("No certificate renewal indicators found in output")
        print(f"Renewal output was:\n{output[:500]}{'...' if len(output) > 500 else ''}")
        print("DECISION: Skipping nginx restart (no renewal detected)")
        return 'skip_restart'

    def send_skip_email_with_days(ti):
        """Send skip email with actual days remaining"""
        print("=== SENDING FRIGG2 SKIP SUCCESS EMAIL ===")
        days_remaining = ti.xcom_pull(task_ids='decide_renewal', key='days_remaining')
        print(f"Retrieved days remaining from XCom: {days_remaining}")
        
        if days_remaining is None:
            print("Warning: Could not retrieve days remaining from XCom, using fallback")
            days_remaining = "more than 7"
        
        subject = 'frigg2 SSL Certificate Check - No Renewal Needed'
        html_content = f"""
        <h3>SSL certificate check completed successfully - no renewal needed.</h3>
        <ul>
            <li>Host: <code>frigg2-mc</code></li>
            <li>Certificate is still valid ({days_remaining} days remaining)</li>
            <li>No container restart was needed</li>
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
        ssh_conn_id='ssh_frigg2',
        command='docker exec docker-compose_nginx_1 certbot certificates',
        do_xcom_push=True,
    )

    # 2. Decide if renewal is needed based on expiry
    decide_renewal = BranchPythonOperator(
        task_id='decide_renewal',
        python_callable=check_expiry_and_decide,
    )

    # 3. Renew certificates inside the nginx container (only if needed)
    renew_certificates = SSHOperator(
        task_id='renew_certificates',
        ssh_conn_id='ssh_frigg2',
        command='docker exec docker-compose_nginx_1 certbot renew --webroot -w /letsencrypt/challenge/',
        do_xcom_push=True,
    )

    # 4. Decide if restart is needed based on renewal
    decide_restart = BranchPythonOperator(
        task_id='decide_restart',
        python_callable=decide_restart_needed,
    )

    # 5. Skip renewal (dummy task)
    skip_renewal = DummyOperator(
        task_id='skip_renewal',
    )

    # 6. Restart the nginx container (only if renewal occurred)
    restart_nginx_container = SSHOperator(
        task_id='restart_nginx_container',
        ssh_conn_id='ssh_frigg2',
        command='docker restart docker-compose_nginx_1',
    )

    # 7. Skip restart (dummy task)
    skip_restart = DummyOperator(
        task_id='skip_restart',
    )

    # 8. Send success email for renewal
    send_renewal_success_email = EmailOperator(
        task_id='send_renewal_success_email',
        to=['mohd.muzamil.08@gmail.com'],
        subject='frigg2 SSL Certificate Renewal Successful',
        html_content="""
        <h3>SSL certificate was renewed successfully on frigg2-mc and nginx container restarted.</h3>
        <ul>
            <li>Host: <code>frigg2-mc</code></li>
            <li>Container: <code>docker-compose_nginx_1</code></li>
            <li>Renewal method: <code>certbot renew --webroot</code></li>
        </ul>
        """,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # 9. Send success email for skipped renewal with actual days remaining
    send_skip_success_email = PythonOperator(
        task_id='send_skip_success_email',
        python_callable=send_skip_email_with_days,
    )

    # 10. Send failure alert if anything breaks
    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to=['mohd.muzamil.08@gmail.com'],
        subject='frigg2 SSL Certificate Renewal FAILED',
        html_content="""
        <h3>SSL Certificate renewal or restart failed on <code>frigg2-mc</code>.</h3>
        <p>Check logs and the system immediately.</p>
        <p>Manual commands to run:</p>
        <ul>
            <li><code>docker exec -it docker-compose_nginx_1 certbot renew --webroot -w /letsencrypt/challenge/</code></li>
            <li><code>docker restart docker-compose_nginx_1</code></li>
        </ul>
        """,
        trigger_rule=TriggerRule.ONE_FAILED
    )

    # DAG task flow
    check_cert_expiry >> decide_renewal
    decide_renewal >> [renew_certificates, skip_renewal]
    renew_certificates >> decide_restart
    decide_restart >> [restart_nginx_container, skip_restart]
    restart_nginx_container >> [send_renewal_success_email, send_failure_email]
    skip_restart >> [send_renewal_success_email, send_failure_email]
    skip_renewal >> send_skip_success_email
