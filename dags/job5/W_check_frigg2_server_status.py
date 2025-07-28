from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
import requests
import subprocess
import re

# Server and container information
SERVER_URL = 'http://frigg2.research.cs.dal.ca/api/rsakey'
CONTAINER_NAMES = ['docker-compose_nginx_1', 'docker-compose_backup_1', 'docker-compose_meteorapp_1', 'docker-compose_mongo_1']

default_args = {
    'owner': 'prositadmin',
    'start_date': datetime(2024, 10, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define a DAG with a schedule interval of 10mins
schedule_interval = '*/30 * * * *'

# Define the DAG
dag = DAG('monitor_frigg2_server', description='Monitor remote server and Docker containers',
          schedule_interval=schedule_interval,
          default_args=default_args,
          catchup=False)

# Airflow variable to store the last known server status
SERVER_STATUS_VAR = 'server_status'
DOCKER_STATUS_VAR = 'docker_status'


def check_server_status():
    try:
        response = requests.get(SERVER_URL, timeout=10)
        if response.status_code == 200:
            return 'up'
        else:
            return 'down'
    except requests.exceptions.RequestException:
        return 'down'

# A function to extract just the core status ("Up" or "Down") from the full status string
def extract_core_status(status):
    # Match "Up" or "Exited" or any core status without the time
    match = re.match(r"(Up|Exited|Down)", status)
    return match.group(1) if match else "Unknown"


# Function to check docker containers
def check_docker_containers(previous_statuses=None):
    try:
        # Get the output of 'docker ps' command
        result = subprocess.run(
            ['ssh', 'mmh@frigg2.research.cs.dal.ca', 'docker ps --format "{{.Names}}: {{.Status}}"'],
            capture_output=True,
            text=True,
            check=True
        )
        container_statuses = result.stdout.strip().split('\n')
        current_status_dict = {}
        
        # Extract the name and core status of each container
        for status in container_statuses:
            name, container_status = status.split(': ')
            core_status = extract_core_status(container_status)
            current_status_dict[name] = core_status

        # If we have previous statuses, compare and detect changes
        if previous_statuses:
            changes = {}
            for name, status in current_status_dict.items():
                if name in previous_statuses and previous_statuses[name] != status:
                    changes[name] = (previous_statuses[name], status)
            
            # Return the changes if there are any
            if changes:
                return changes, current_status_dict
        
        # If no changes or no previous status available
        return None, current_status_dict

    except subprocess.CalledProcessError as e:
        # Log the error output for debugging
        error_message = e.stderr.strip() if e.stderr else "Unknown error occurred"
        print(f"Error occurred while checking Docker containers: {error_message}")
        return str(error_message), {}
    except Exception as e:
        return str(e), {}


def monitor_server(**kwargs):
    ti = kwargs['ti']
    
    # Check the server status
    server_status = check_server_status()
    last_status = Variable.get(SERVER_STATUS_VAR, default_var='up')
    print(f"Current Server status: {server_status}")

    # Initialize status message list
    status_message = []

    # Check if the server status has changed
    if server_status != last_status:
        # Update the status in the Airflow variable
        Variable.set(SERVER_STATUS_VAR, server_status)
        
        if server_status == 'down':
            status_message.append('Frigg2 Server is Down')
            ti.xcom_push(key='send_email_subject', value='Frigg2 Server is Down')
        elif server_status == 'up':
            ti.xcom_push(key='send_email_subject', value='Frigg2 Server is Back Online')
            status_message.append('Frigg2 Server is Back Online')

    # Fetch the last known Docker container status from the Airflow variable
    last_docker_status = Variable.get(DOCKER_STATUS_VAR, default_var='{}')  # Stored as a string
    last_docker_status = eval(last_docker_status)  # Convert back to dictionary

    # Check Docker containers status using the updated method
    changes, current_docker_status = check_docker_containers(previous_statuses=last_docker_status)
    
    print(f"Current Docker status: {current_docker_status}")

    # If Docker container statuses have changed or if there's an error message
    if changes:
        # Update the status in the Airflow variable
        Variable.set(DOCKER_STATUS_VAR, str(current_docker_status))  # Store as a string
        
        # If there are changes in Docker containers
        if isinstance(changes, dict):
            container_status_message = "\n".join([f"{name}: {old_status} -> {new_status}" for name, (old_status, new_status) in changes.items()])
            status_message.append(f'Docker container status has changed:\n{container_status_message}')
        else:
            # If there's an error (though 'changes' should normally be a dict)
            status_message.append(f'Error checking Docker containers: {changes}')
    
    # Create a single email alert message if status_message is not empty
    if status_message:
        full_status_message = " | ".join(status_message)
        # Send the alert email
        ti.xcom_push(key='send_email_body', value=full_status_message)
        return 'send_email'
    else:
        return 'skip_email'


monitor_task = PythonOperator(
    task_id='monitor_server',
    python_callable=monitor_server,
    provide_context=True,
    dag=dag
)

# Branching task to decide whether to send email or skip
branch_task = BranchPythonOperator(
    task_id='branch_decision',
    python_callable=lambda **kwargs: kwargs['ti'].xcom_pull(task_ids='monitor_server'),
    provide_context=True,
    dag=dag
)

# emailing to Muzamil, Sandra, Shuya, Dawson, Sumit
email_list = ['mohd.muzamil.08@gmail.com']#, 'sandra.meier@iwk.nshealth.ca', 'Nathan.Rowarth@Dal.Ca', 'sh678384@dal.ca', 'sm585558@dal.ca']
# email_list = email_list[0]  # delete this line to send email to all, only for testing

email = EmailOperator(
    task_id='send_email',
    to=email_list,
    subject="{{ ti.xcom_pull(task_ids='monitor_server', key='send_email_subject') }}",
    html_content=""" <h3>Server Monitoring Alert:</h3>
                     <p>{{ ti.xcom_pull(task_ids='monitor_server', key='send_email_body') }}</p> """,
    trigger_rule='one_success',  # Trigger when `monitor_task` sets an email message
    dag=dag
)

# DummyOperator to skip email
skip_email = DummyOperator(
    task_id='skip_email',
    dag=dag
)

# Set task dependencies
monitor_task >> branch_task
branch_task >> [email, skip_email]
