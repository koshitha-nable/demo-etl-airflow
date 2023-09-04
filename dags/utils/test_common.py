import unittest
from unittest.mock import MagicMock, patch  # For mocking dependencies like psycopg2

# Import your functions to be tested
from common import (
    final_status_func,
    handle_failure,
    send_notification,
    create_postgres_connection,
    close_postgres_connection,
)

class TestCommonFunctions(unittest.TestCase):

    
    def test_create_postgres_connection(self):
        # Mock the psycopg2.connect method
        with patch('psycopg2.connect') as mock_connect:
            mock_connection = mock_connect.return_value

            # Mock the Variable.get method if needed
            with patch('airflow.models.Variable.get') as mock_get:
                # Set the return values for the Variable.get calls
                mock_get.side_effect = ["your_user", "your_password", "your_db_name"]

                # Call the function
                connection = create_postgres_connection()

                # Assert that the connection is established
                self.assertIsNotNone(connection)
    
    def test_close_postgres_connection(self):
        # Create a mock PostgreSQL connection
        mock_connection = MagicMock()

        # Call the function and verify that it closes the connection
        close_postgres_connection(mock_connection)
        mock_connection.close.assert_called_once()


    def test_final_status_func_success(self):
        # Create a mock task_instance with a SUCCESS state
        mock_task_instance = MagicMock()
        mock_task_instance.current_state.return_value = "success"
        
        # Create a mock dag_run with the mock task_instance
        mock_dag_run = MagicMock()
        mock_dag_run.get_task_instances.return_value = [mock_task_instance]

        # Create a mock context with the mock dag_run
        mock_context = {
            'dag_run': mock_dag_run,
            'task_instance': mock_task_instance,
        }

        # Call the function and assert that it doesn't raise an exception
        final_status_func(**mock_context)


    def test_handle_failure(self):
            # Mock the necessary dependencies, e.g., send_notification
            with patch('common.send_notification') as mock_send_notification:
                # Create a mock context with a failed task
                mock_context = {'task_instance': MagicMock()}

                # Call the function and verify that send_notification is called
                handle_failure(mock_context)
                mock_send_notification.assert_called_once()


    @patch('smtplib.SMTP')
    def test_send_notification_success(self, mock_smtp):
        # Mock the SMTP server and login
        instance = mock_smtp.return_value
        instance.starttls.return_value = None
        instance.login.return_value = None

        # Set up the input parameters for send_notification
        failed_task_id = 'task123'

        # Call the send_notification function
        send_notification(failed_task_id)

        # Assertions
        instance.send_message.assert_called_once()
        self.assertEqual(instance.send_message.call_args[0][0]['To'], 'koshithaa@n-able.biz')
        self.assertEqual(instance.send_message.call_args[0][0]['Subject'], 'Airflow DAG Execution Failure')

    @patch('smtplib.SMTP')
    def test_send_notification_failure(self, mock_smtp):
        # Mock the SMTP server and raise an exception when sending the message
        instance = mock_smtp.return_value
        instance.starttls.return_value = None
        instance.login.return_value = None
        instance.send_message.side_effect = Exception('SMTP error')

        # Set up the input parameters for send_notification
        failed_task_id = 'task123'

        # Call the send_notification function
        with self.assertRaises(Exception) as context:
            send_notification(failed_task_id)

        # Assertions
        self.assertEqual(str(context.exception), 'SMTP error')




   

