import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from sql import *
from utils.staging import *
from utils.common import *


with DAG(
    dag_id='api_dag',
    schedule_interval='@daily',
    start_date= days_ago(1),
    catchup=False,
    on_failure_callback=handle_failure  # Specify the failure handling function
) as dag:
    
    task_is_api_active = HttpSensor(
        task_id='is_api_active',
        http_conn_id='mock-data-server-connection',
        endpoint='users'
    )

    with TaskGroup("check_api_endpoints") as check_api_endpoints_group:

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
            endpoint='transactions',
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
                bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'get_users\') }}"', 
            )
        
        show_data_task = BashOperator(
                task_id='show_product_data_task',
                bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'get_products\') }}"', 
            )
        
        show_data_task = BashOperator(
                task_id='show_transaction_data_task',
                bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'get_transactions\') }}"', 
            )
        
    task_check_csv = PythonOperator(

        task_id="check_csv",
        python_callable=get_reviews

    )

    task_show_csv_data = BashOperator(
                task_id='show_src_reviews_data',
                bash_command='echo "{{ task_instance.xcom_pull(task_ids=\'check_csv\') }}"',  
            )
    
    with TaskGroup("load_src_api_to_db") as load_src_api_data_group:
        
        task_load_src_user = PythonOperator(
            task_id="load_user_to_stg",
            python_callable=load_user_data_to_db
        )

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
    
    trigger_intermediate_dag = TriggerDagRunOperator(
        task_id='trigger_intermediate_dag',
        trigger_dag_id="int_dag",
        execution_date = '{{ ds }}',
        reset_dag_run = True
    )

    trigger_dimension_dag = TriggerDagRunOperator(
        task_id='trigger_idim_dag',
        trigger_dag_id="dim_dag",
        execution_date = '{{ ds }}',
        reset_dag_run = True
    )
    
    final_status = PythonOperator(
        task_id='final_status',
        provide_context=True,
        python_callable=final_status_func,
        trigger_rule=TriggerRule.ALL_DONE, # Ensures this task runs even if upstream fails
    )
       
task_is_api_active >>check_api_endpoints_group >> [create_stg_tables_group, check_api_data_group]  >>  load_src_api_data_group >> task_check_csv >> task_show_csv_data >> task_load_src_review >> trigger_intermediate_dag >> trigger_dimension_dag >> final_status