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
from airflow.models.base import Base
from airflow.utils.trigger_rule import TriggerRule
from sql import *
from utils.dimension import *
from utils.common import *


with DAG(
    dag_id='dim_dag',
    schedule_interval='@daily',
    start_date= days_ago(1),
    catchup=False,
    on_failure_callback=handle_failure
    
) as dag:
    
    with TaskGroup("create_table") as create_dim_table_group:
    
        task_create_int_user = PostgresOperator(
                task_id="create_users_int",  
                postgres_conn_id='mock_remote_db',
                sql='sql/create_users_dim.sql'
            )
        
        task_create_int_product = PostgresOperator(
                task_id="create_products_int",  
                postgres_conn_id='mock_remote_db',
                sql='sql/create_products_dim.sql'
            )
        
        task_create_int_transaction = PostgresOperator(
                task_id="create_transaction_int",  
                postgres_conn_id='mock_remote_db',
                sql='sql/create_transactions_fact.sql'
            )
        
        task_create_int_reviews = PostgresOperator(
                task_id="create_review_int",  
                postgres_conn_id='mock_remote_db',
                sql='sql/create_review_dim.sql')
        
    task_load_fact_trans = PythonOperator(
            task_id="load_fact_transaction",
            python_callable=load_fact_transaction
        )
    
    task_load_dim_users = PythonOperator(
            task_id="load_dim_user",
            python_callable=load_dim_user
        )
    
    task_load_dim_products = PythonOperator(
            task_id="load_dim_product",
            python_callable=load_dim_product
        )
    
    task_load_dim_reviews = PythonOperator(
            task_id="load_dim_review",
            python_callable=load_dim_review
        )
        
        
    final_status = PythonOperator(
        task_id='final_status',
        provide_context=True,
        python_callable=final_status,
        trigger_rule=TriggerRule.ALL_DONE, # Ensures this task runs even if upstream fails
        )
    
create_dim_table_group >> task_load_dim_users >> task_load_dim_products >> task_load_dim_reviews >> task_load_fact_trans >> final_status