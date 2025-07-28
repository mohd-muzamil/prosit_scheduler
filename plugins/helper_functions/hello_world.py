import logging

def hello():
    print("Hello World!...")
    task_logger = logging.getLogger("airflow.task")
    task_logger.info("This log is informational")
    task_logger.warning("This log is a warning")
    task_logger.error("This log shows an error!")
    task_logger.critical("This log shows a critical error!")
