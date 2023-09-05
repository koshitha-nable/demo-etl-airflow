from airflow.models import DagBag
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.task_group import TaskGroup
import pendulum

import unittest
from unittest.mock import patch

class TestMyDag(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder='/opt/airflow/dags') 

    ## Asserting that the DAG is created correctly

    def test_etl_dag_starts_on_correct_date(self):
        etl_dag = self.dagbag.get_dag('api_dag')
        expected_start_date = pendulum.now().subtract(days=1).start_of('day')
        actual_start_date = etl_dag.start_date
        assert actual_start_date == expected_start_date

    def test_etl_dag_has_catchup_set_to_true(self):
        etl_dag = self.dagbag.get_dag('api_dag')
        assert etl_dag.catchup  == False 

    def test_etl_dag_runs_schedule(self):
        etl_dag = self.dagbag.get_dag('api_dag')
        assert etl_dag.schedule_interval == '@daily'


    def test_dag_loaded(self):
        dag = self.dagbag.get_dag('api_dag')
        assert self.dagbag.import_errors == {}
        assert dag is not None
        # Update the expected_task_count based on your actual DAG structure
        expected_task_count = 20 
        assert len(dag.tasks) == expected_task_count

    def test_task_count(self):
        dag_id = 'api_dag'  
        dag = self.dagbag.get_dag(dag_id)
        self.assertIsNotNone(dag)
        # Test the number of tasks in the DAG
        self.assertEqual(len(dag.tasks), 20)  

    #Testing HttpSensor and SimpleHttpOperator:

    def test_http_sensor_task(self):
        dag = self.dagbag.get_dag('api_dag')
        task = dag.get_task('is_api_active')
        self.assertIsInstance(task, HttpSensor)
        self.assertEqual(task.task_id, 'is_api_active')
        self.assertEqual(task.http_conn_id, 'mock-data-server-connection')
        self.assertEqual(task.endpoint, 'users')

    #Asserting that tasks are ordered correctly

    def test_etl_dag_has_correct_task_order(self):
        etl_dag = self.dagbag.get_dag('api_dag')
        int_task = etl_dag.get_task('trigger_intermediate_dag')
        dim_task = etl_dag.get_task('trigger_idim_dag')
       
        assert int_task.downstream_list == [dim_task]

    #Asserting that tasks are triggering on the correct rules

    def test_transform_only_run_when_extract_is_successful(self):
        etl_dag =self.dagbag.get_dag('api_dag')
        int_task = etl_dag.get_task('trigger_intermediate_dag')
        assert int_task.trigger_rule == TriggerRule.ALL_SUCCESS

    def test_transform_only_run_when_transformation_is_successful(self):
        etl_dag =self.dagbag.get_dag('api_dag')
        dim_task = etl_dag.get_task('trigger_idim_dag')
        assert dim_task.trigger_rule == TriggerRule.ALL_SUCCESS

    def test_http_task(self):
        dag = self.dagbag.get_dag('api_dag')
        tg = dag.task_group.get_child_by_label('check_api_endpoints')
        #task = tg.get_task('get_users')
        task=tg.get_task('get_users')
        assert task.trigger_rule == TriggerRule.ALL_SUCCESS
        

 


   
if __name__ == '__main__':
    unittest.main()
