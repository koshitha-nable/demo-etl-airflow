import unittest
from unittest.mock import patch
import pandas as pd

import sys,os

# Add the parent directory of your project to the Python path
project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_path)

from dags.utils.staging import (
    load_user_data_to_db,
    load_product_data_to_db,
    load_transaction_data_to_db,
    load_review_to_db,
    pull_user_data,
    pull_product_data,
    pull_transaction_data,
    get_reviews,
)


class TestStgCode(unittest.TestCase):

    @patch('dags.utils.staging.create_postgres_connection')
    @patch('dags.utils.staging.pull_transaction_data')
    @patch('dags.utils.staging.close_postgres_connection')
    def test_load_transaction_data_to_db(self, mock_create_connection, mock_pull_transaction_data, mock_close_connection):
        # Mock the database connection and data retrieval
        mock_connection = mock_create_connection.return_value
        mock_cursor = mock_connection.cursor.return_value
        mock_pull_transaction_data.return_value = pd.DataFrame({'transaction_id': [1, 2], 'amount': [100, 200]})

        # Call the function to be tested
        load_transaction_data_to_db()

        # Assertions
        mock_create_connection.assert_called_once()
        mock_pull_transaction_data.assert_called_once()
        mock_cursor.execute.assert_not_called()  # The function doesn't execute any SQL statement
        mock_close_connection.assert_called_once()


    @patch('dags.utils.staging.create_postgres_connection')
    @patch('dags.utils.staging.pull_product_data')
    @patch('dags.utils.staging.close_postgres_connection')
    def test_load_product_data_to_db(self, mock_create_connection, mock_pull_product_data, mock_close_connection):
        # Mock the database connection and data retrieval
        mock_connection = mock_create_connection.return_value
        mock_cursor = mock_connection.cursor.return_value
        mock_pull_product_data.return_value = pd.DataFrame({'product_id': [1, 2], 'name': ['Product1', 'Product2']})

        # Call the function to be tested
        load_product_data_to_db()

        # Assertions
        mock_create_connection.assert_called_once()
        mock_pull_product_data.assert_called_once()
        mock_cursor.execute.assert_not_called()  # The function doesn't execute any SQL statement
        mock_close_connection.assert_called_once()
    

    @patch('dags.utils.staging.create_postgres_connection')
    @patch('dags.utils.staging.get_reviews')
    @patch('dags.utils.staging.close_postgres_connection')
    def test_load_review_to_db(self, mock_create_connection, mock_get_reviews, mock_close_connection):
        # Mock the database connection and data retrieval
        mock_connection = mock_create_connection.return_value
        mock_cursor = mock_connection.cursor.return_value
        mock_get_reviews.return_value = pd.DataFrame({'review_id': [1, 2], 'rating': [4, 5]})

        # Call the function to be tested
        load_review_to_db()

        # Assertions
        mock_create_connection.assert_called_once()
        mock_get_reviews.assert_called_once()
        mock_cursor.execute.assert_not_called()  # The function doesn't execute any SQL statement
        mock_close_connection.assert_called_once()
    
    def test_pull_user_data_failure(self):
        # Mock a scenario where pulling user data fails
        with patch('dags.utils.staging.requests.get') as mock_get:
            mock_get.side_effect = Exception("Data retrieval error")
            with self.assertRaises(Exception):
                pull_user_data()


    def test_pull_product_data_failure(self):
        # Mock a scenario where pulling product data fails
        with patch('dags.utils.staging.requests.get') as mock_get:
            mock_get.side_effect = Exception("Data retrieval error")
            with self.assertRaises(Exception):
                pull_product_data()

    
    def test_pull_transaction_data_failure(self):
        # Mock a scenario where pulling product data fails
        with patch('dags.utils.staging.requests.get') as mock_get:
            mock_get.side_effect = Exception("Data retrieval error")
            with self.assertRaises(Exception):
                pull_transaction_data()

    def test_get_reviews(self):
        # Ensure that the function returns a DataFrame
        review_data = get_reviews()
        self.assertIsInstance(review_data, pd.DataFrame)


    @patch('dags.utils.staging.create_postgres_connection')
    @patch('dags.utils.staging.pull_user_data')
    @patch('dags.utils.staging.close_postgres_connection')
    def test_load_user_data_to_db(self, mock_create_connection, mock_pull_user_data, mock_close_connection):
        # Mock the database connection and data retrieval
        mock_create_connection.return_value.cursor.return_value.execute.return_value = None
        mock_pull_user_data.return_value = pd.DataFrame({'user_id': [1, 2], 'name': ['User1', 'User2']})

        # Call the function to be tested
        load_user_data_to_db()

        # Assertions
        mock_create_connection.assert_called_once()
        mock_pull_user_data.assert_called_once()
        mock_close_connection.assert_called_once()


if __name__ == '__main__':
    unittest.main()
