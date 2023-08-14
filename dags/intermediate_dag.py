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
from utils.intermediate import *

with DAG(
    dag_id='int_dag',
    schedule_interval='@daily',
    start_date= days_ago(1),
    catchup=False
) as dag:
    
    task_load_stg_user = PythonOperator(
             task_id="load_stg_user",
            python_callable=load_user_data_to_inter
        )
    
    task_load_stg_product = PythonOperator(
             task_id="load_stg_product",
            python_callable=load_product_data_to_inter
        )
    
    task_load_stg_transaction = PythonOperator(
             task_id="load_stg_transaction",
            python_callable=load_transaction_data_to_inter
        )
    
    task_load_stg_review = PythonOperator(
             task_id="load_stg_review",
            python_callable=load_review_data_to_inter
        )
    
task_load_stg_user >> task_load_stg_product >> task_load_stg_transaction >> task_load_stg_review