import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from sql import *

with DAG(
    dag_id='api_dag',
    schedule_interval='@daily',
    start_date= days_ago(1),
    catchup=False
) as dag:
    
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='mock-data-server-connection',
        endpoint='users'
    )

    task_get_users = SimpleHttpOperator(
        task_id='get_users',
        http_conn_id='mock-data-server-connection',
        endpoint='users',
        method='GET',
        do_xcom_push=True,
        response_filter=lambda response: json.loads(response.text),
        log_response=True

    )

    task_get_transactions = SimpleHttpOperator(
        task_id='get_transactions',
        http_conn_id='mock-data-server-connection',
        endpoint='products',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    task_get_products = SimpleHttpOperator(
        task_id='get_products',
        http_conn_id='mock-data-server-connection',
        endpoint='products',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True

    )

    with TaskGroup("create_staging_tables") as create_stg_tables_group:
        extract_users_task = PostgresOperator(
            task_id="create_users_stg",  
            postgres_conn_id='mock_remote_db',
            sql='sql/create_users_src.sql'
        )

        extract_products_task = PostgresOperator(
            task_id="create_products_stg", 
            postgres_conn_id='mock_remote_db',
            sql='sql/create_products_src.sql'
        )

        extract_transaction_task = PostgresOperator(
            task_id="create_transaction_stg",
            postgres_conn_id='mock_remote_db',
            sql='sql/create_transactions_src.sql')

       
task_is_api_active >>[ task_get_users , task_get_products,task_get_transactions] >> create_stg_tables_group