from airflow.utils.state import State
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from airflow.models import Variable
from sqlalchemy.sql.type_api import Variant

def final_status(**kwargs):
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() != State.SUCCESS and \
                task_instance.task_id != kwargs['task_instance'].task_id:
            raise Exception("Task {} failed. Failing this DAG run".format(task_instance.task_id))
        

def handle_failure(context):
    # This function will be called whenever a task fails in the DAG
    failed_task = context.get('task_instance')
    failed_task_id = failed_task.task_id

    # Perform actions or create additional tasks specific to handling failure scenarios
    # For example, you can send a notification, trigger a recovery process, or perform cleanup tasks.

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
    except Exception as e:
        print(f'Failed to send email notification. Error: {str(e)}')


def create_postgres_connection():
    
    try:
        connection = psycopg2.connect(
            user=Variable.get("POSTGRES_USER"),
            password=Variable.get("POSTGRES_PASSWORD"),
            host="remote_db",
            database=Variable.get("DB_NAME")
        )
        return connection
        
    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        return None

def close_postgres_connection(connection):
    try:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")
    except Exception as error:
        print("Error while closing PostgreSQL connection:", error)
