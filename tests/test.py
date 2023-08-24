import unittest
from unittest.mock import Mock, patch
from airflow.models import DagBag


class TestHttpSensorTask(unittest.TestCase):

    # def setUp(self):
    #     self.dagbag = DagBag(dag_folder="/path/to/your/dags")  # Path to your DAGs folder

    def setUp(self):
        self.dagbag = DagBag()

    def test_task_count(self):
        """Check task count of hello_world dag"""
        dag_id = 'api_dag'
        dag = self.dagbag.get_dag(dag_id)
        self.assertEqual(len(dag.tasks), 20)

    def test_http_sensor_task(self):
        dag = self.dagbag.get_dag('api_dag')
        task = dag.get_task('is_api_active')
        self.assertIsNotNone(task)
        self.assertEqual(task.task_id, 'is_api_active')
        self.assertEqual(task.http_conn_id, 'mock-data-server-connection')
        self.assertEqual(task.endpoint, 'users')

        # Assuming you have configured other properties of the HttpSensor task,
        # you can add more assertions here based on your DAG's configuration.

    
    @patch('airflow.providers.postgres.operators.postgres.PostgresHook')  # Mock PostgresHook
    def test_create_users_stg_task(self, mock_postgres_hook):
        dag_run = self.dagbag.get_dag("api_dag").create_dagrun(
            run_id='test_run',
            execution_date=datetime(2023, 8, 24),
            state='running'
        )
        dag_run.dag.clear()

        # Mock the behavior of PostgresHook
        mock_postgres_hook_instance = Mock()
        mock_postgres_hook.return_value = mock_postgres_hook_instance

        # Run the operator
        PostgresOperator(task_id="create_users_stg").execute(context={})

        # Assert the expected behavior
        mock_postgres_hook_instance.run.assert_called_once()
        # Add more assertions based on your operator's logic

    # Similar test functions for create_products_stg_task, create_transaction_stg_task, and create_review_stg_task

if __name__ == '__main__':
    unittest.main()
