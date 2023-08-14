import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from sql import *
from utils.etl_utils import *

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
        
        extract_review_task = PostgresOperator(
            task_id="create_review_stg",
            postgres_conn_id='mock_remote_db',
            sql='sql/create_review_src.sql')

        
    with TaskGroup("check_api_data") as check_api_data_group:
        
        show_data_task = BashOperator(
                task_id='show_user_data_task',
                bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'get_users\') }}"',  # Modify the command as needed
            )
        
        show_data_task = BashOperator(
                task_id='show_product_data_task',
                bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'get_products\') }}"',  # Modify the command as needed
            )
        
        show_data_task = BashOperator(
                task_id='show_transaction_data_task',
                bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'get_transactions\') }}"',  # Modify the command as needed
            )
    task_check_csv = PythonOperator(

        task_id="check_csv",
        python_callable=get_reviews

    )

    task_show_csv_data = BashOperator(
                task_id='show_src_reviews_data',
                bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'check_csv\') }}"',  # Modify the command as needed
            )
    
    with TaskGroup("load_src_api_to_db") as load_src_api_data_group:
        
        # task_load_src_user = PythonOperator(
        #     task_id="load_user_to_stg",
        #     python_callable=load_user_data_to_db
        # )

        task_load_src_product = PythonOperator(
            task_id="load_product_to_stg",
            python_callable=load_product_data_to_db
        )

        task_load_src_transactions = PythonOperator(
            task_id="load_transaction_to_stg",
            python_callable=load_transaction_data_to_db
        )

    task_load_src_review = PythonOperator(
             task_id="load_review_to_stg",
            python_callable=load_review_to_db
        )
       
task_is_api_active >> [ task_get_users , task_get_products,task_get_transactions] >> create_stg_tables_group >> check_api_data_group >> task_check_csv >> task_show_csv_data >> task_load_src_review>> load_src_api_data_group