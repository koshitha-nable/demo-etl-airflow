import unittest
from unittest.mock import patch,MagicMock
from unittest.mock import Mock, patch
import logging
import os
import pandas as pd
from sqlalchemy.engine import Connection
import sys,os

# Add the parent directory of your project to the Python path
project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_path)

from dags.utils.dimension import (
    con,
    logger,
    validate_fact,
    validate_dim_product,
    validate_dim_user,
    validate_dim_review,
    load_fact_transaction,
    load_dim_product,
    load_dim_user,
    load_dim_review,
)

class TestDimModule(unittest.TestCase):

    def setUp(self):
        # Create a DataFrame with no null values for testing
        self.valid_df_fact = pd.DataFrame({'column1': [1, 2, 3], 'column2': ['A', 'B', 'C']})
        self.valid_df_product = pd.DataFrame({'product_id': [1, 2, 3], 'product_name': ['A', 'B', 'C']})
        self.valid_df_user = pd.DataFrame({'user_id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']})
        self.valid_df_review = pd.DataFrame({'review_id': [1, 2, 3], 'product_id': [1, 2, 3], 'review_score': [4, 5, 4], 'review_date': ['2023-01-01', '2023-01-02', '2023-01-03']})
        self.sample_data_fact = pd.DataFrame({
            'purchase_id': [1, 2, 3],
            'product_id': [101, 102, 103],
            'user_id': [201, 202, 203],
            'quantity': [5, 3, 4],
            'total_amount': [100.0, 75.0, 120.0]
        })

        self.sample_data_product = pd.DataFrame({
            'product_id': [101, 102, 103],
            'product_name': ['Product A', 'Product B', 'Product C'],
            'price': [10.0, 15.0, 20.0],
            'discounted_price': [8.0, 12.0, 18.0],
            'product_description': ['Description A', None, 'Description C']
        })


        self.sample_data_user = pd.DataFrame({
            'user_id': [201, 202, 203],
            'name': ['User A', 'User B', 'User C'],
            'email': ['usera@example.com', 'userb@example.com', 'userc@example.com'],
            'zip_code': ['12345', '67890', '54321'],
            'state': ['FL', 'CA', None]
        })

        self.sample_data_review = pd.DataFrame({
            'review_id': [301, 302, 303],
            'product_id': [101, 102, 103],
            'review_score': [4, 5, 3],
            'review_date': ['2023-01-01', '2023-02-01', '2023-03-01']
        })


    def test_load_fact_transaction_success(self):
        # Mock the logger
        test_logger = unittest.mock.Mock()
        with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
            with unittest.mock.patch('pandas.read_sql_table', return_value=self.sample_data_fact):
                with unittest.mock.patch('pandas.DataFrame.to_sql'):
                    # Call the load_fact_transaction function
                    load_fact_transaction()

        # Check that the logger info message is correct
        test_logger.info.assert_called_once_with("Loading transaction data to fact table...")


    def test_load_dim_product_success(self):
        # Mock the logger
        test_logger = unittest.mock.Mock()
        with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
            with unittest.mock.patch('pandas.read_sql_table', return_value=self.sample_data_product):
                with unittest.mock.patch('pandas.DataFrame.to_sql'):
                    # Call the load_dim_product function
                    load_dim_product()

        # Check that the logger info message is correct
        test_logger.info.assert_called_once_with("Loading product data to dim table...")


    def test_load_dim_user_success(self):
            # Mock the logger
        test_logger = unittest.mock.Mock()
        with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
            with unittest.mock.patch('pandas.read_sql_table', return_value=self.sample_data_user):
                with unittest.mock.patch('pandas.DataFrame.to_sql'):
                    # Call the load_dim_user function
                    load_dim_user()

        # Check that the logger info message is correct
        test_logger.info.assert_called_once_with("Loading user data to dim table...")


    def test_load_dim_review_success(self):
            # Mock the logger
            test_logger = unittest.mock.Mock()
            with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
                with unittest.mock.patch('pandas.read_sql_table', return_value=self.sample_data_review):
                    with unittest.mock.patch('pandas.DataFrame.to_sql'):
                        # Call the load_dim_review function
                        load_dim_review()

            # Check that the logger info message is correct
            test_logger.info.assert_called_once_with("Loading review data to dim table...")
    
    
    def test_valid_fact_table(self):
        # Mock the logger
        test_logger = unittest.mock.Mock()
        with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
            with unittest.mock.patch('pandas.read_sql_table', return_value=self.valid_df_fact):
                # Call the validate_fact function
                validate_fact()

        # Check that the logger info message is correct
        test_logger.info.assert_called_once_with("fact table is validated")


    def test_invalid_fact_table(self):
        # Create a DataFrame with null values for testing
        invalid_df = pd.DataFrame({'column1': [1, 2, None], 'column2': ['A', None, 'C']})

        # Mock the logger
        test_logger = unittest.mock.Mock()
        with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
            with unittest.mock.patch('pandas.read_sql_table', return_value=invalid_df):
                # Use assertRaises to check if the function raises an exception
                with self.assertRaises(Exception) as context:
                    validate_fact()

        # Check that the logger error message is correct
        test_logger.error.assert_called_once_with("Error.. Null Values Found")
        # Check that the exception message is correct
        self.assertEqual(str(context.exception), "Null values found in the fact table!")


    def test_valid_dim_product_table(self):
        # Mock the logger
        test_logger = unittest.mock.Mock()
        with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
            with unittest.mock.patch('pandas.read_sql_table', return_value=self.valid_df_product):
                # Call the validate_dim_product function
                validate_dim_product()

        # Check that the logger info message is correct
        test_logger.info.assert_called_once_with("Dimension table 'dim_product' validated successfully.")


    def test_valid_dim_user_table(self):
        # Mock the logger
        test_logger = unittest.mock.Mock()
        with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
            with unittest.mock.patch('pandas.read_sql_table', return_value=self.valid_df_user):
                # Call the validate_dim_user function
                validate_dim_user()

        # Check that the logger info message is correct
        test_logger.info.assert_called_once_with("Dimension table 'dim_user' validated successfully.")


    def test_valid_dim_review_table(self):
        # Mock the logger
        test_logger = unittest.mock.Mock()
        with unittest.mock.patch('dags.utils.dimension.logger', test_logger):
            with unittest.mock.patch('pandas.read_sql_table', return_value=self.valid_df_review):
                # Call the validate_dim_review function
                validate_dim_review()

        # Check that the logger info message is correct
        test_logger.info.assert_called_once_with("Dimension table 'dim_review' validated successfully.")

    
        
if __name__ == '__main__':
    unittest.main()
