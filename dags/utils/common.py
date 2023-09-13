from airflow.utils.state import State
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
import logging
from sqlalchemy import create_engine
import pandas as pd
from airflow.models import Variable
from sqlalchemy.sql.type_api import Variant

import os

log_directory = "/opt/airflow/logs/custom_dag_logs"
# if not os.path.exists(log_directory):
#     os.makedirs(log_directory,exist_ok=True)

# Create a custom log formatter
log_format = "%(asctime)s [%(levelname)s] - %(message)s"
date_format = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(log_format, datefmt=date_format)

# Create a logger and set its level
logger = logging.getLogger("custom_logger_cmn")
logger.setLevel(logging.DEBUG)

# Define the full path to the log file in the desired directory
#log_file_path = os.path.join(log_directory, "custom_log.log")
log_file_path = os.path.join(log_directory, "combined_log.log")


# Create a FileHandler to write logs to the specified file path
file_handler = logging.FileHandler(log_file_path)

# Set the formatter for the file handler
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

def final_status_func(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() != State.SUCCESS and \
                task_instance.task_id != kwargs['task_instance'].task_id:
            raise Exception("Task {} failed. Failing this DAG run".format(task_instance.task_id))
        

def handle_failure(context):
    # This function will be called whenever a task fails in the DAG
    failed_task = context.get('task_instance')
    failed_task_id = failed_task.task_id

    # Send a notification
    send_notification(failed_task_id)
        

def send_notification(failed_task_id):
    # Email configuration
    sender_email = 'xxx.a@gmail.com'
    recipient_email = 'koshithaa@n-able.biz'
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    smtp_username = 'xxx.a@gmail.com'
    smtp_password = ''

    # Email content
    subject = 'Airflow DAG Execution Failure'
    body = f"An error occurred while executing task '{failed_task_id}' in the DAG. Please check the logs for more details."

    # Construct the email message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    # Send the email
    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(message)
        print('Email notification sent successfully!')
        logger.info("Email notification sent successfully!")
        return True
    except Exception as e:
        print(f'Failed to send email notification. Error: {str(e)}')
        logger.error("Failed to send email notification")
        return False


def create_postgres_connection():
    
    try:
        connection = psycopg2.connect(
            user=Variable.get("POSTGRES_USER"),
            password=Variable.get("POSTGRES_PASSWORD"),
            host="remote_db",
            database=Variable.get("DB_NAME")
        )
        logger.info(" connecting to PostgreSQL")
        return connection
        
        
    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while connecting to PostgreSQL")
        return None
        
    

def close_postgres_connection(connection):
    try:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")
            return True
        
    except Exception as error:
        print("Error while closing PostgreSQL connection:", error)
        logger.error("Error while closing PostgreSQL connection")
        return False
