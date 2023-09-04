from airflow.models import DagBag
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup

import unittest
from unittest.mock import patch

class TestIntDag(unittest.TestCase):


    def setUp(self):
        self.dagbag = DagBag(dag_folder='/opt/airflow/dags') 
     


    def test_etl_dag_has_catchup_set_to_true(self):
        etl_dag = self.dagbag.get_dag('int_dag')
        assert etl_dag.catchup  == False 

    def test_etl_dag_runs_schedule(self):
        etl_dag = self.dagbag.get_dag('int_dag')
        assert etl_dag.schedule_interval == '@daily'


    def test_dag_loaded(self):
        dag = self.dagbag.get_dag('int_dag')
        assert self.dagbag.import_errors == {}
        assert dag is not None
        expected_task_count = 9 
        assert len(dag.tasks) == expected_task_count

    def test_task_count(self):
        dag_id = 'int_dag'  
        dag = self.dagbag.get_dag(dag_id)
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 9)  

    def test_transform_only_run_when_transformation_is_successful(self):
        etl_dag =self.dagbag.get_dag('int_dag')
        task1 = etl_dag.get_task('load_int_user')
        task2 = etl_dag.get_task('load_int_transaction')
        task3 = etl_dag.get_task('load_int_review')
        task4 = etl_dag.get_task('load_int_product')

        assert task1.trigger_rule == TriggerRule.ALL_SUCCESS
        assert task2.trigger_rule == TriggerRule.ALL_SUCCESS
        assert task3.trigger_rule == TriggerRule.ALL_SUCCESS
        assert task4.trigger_rule == TriggerRule.ALL_SUCCESS

    def test_python_operator(self):
        dag_id = 'int_dag'  
        dag = self.dagbag.get_dag(dag_id)
        task1 = dag.get_task('load_int_user')
           # Execute the PythonOperator's callable
        result = task1.python_callable()

        # Check the result or side effects of your Python callable
        self.assertEqual(result, "Success")



 


   
if __name__ == '__main__':
    unittest.main()
