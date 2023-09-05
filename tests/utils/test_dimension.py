import unittest
from unittest.mock import patch
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


    def test_validate_fact_no_null_values(self):
        # Create a test DataFrame with no null values
        test_df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['A', 'B', 'C']})

        with patch('pandas.read_sql_table', return_value=test_df):
            with self.assertLogs(logger, level='INFO') as logs:
                validate_fact()

        # Ensure the log message indicates successful validation
        self.assertIn("fact table is validated", logs.output)

    def test_validate_fact_with_null_values(self):
        # Create a test DataFrame with null values
        test_df = pd.DataFrame({'col1': [1, 2, None], 'col2': ['A', None, 'C']})

        with patch('pandas.read_sql_table', return_value=test_df):
            with self.assertLogs(logger, level='ERROR') as logs:
                with self.assertRaises(Exception) as context:
                    validate_fact()

        # Ensure the log message indicates the presence of null values and an exception is raised
        self.assertIn("Error.. Null Values Found", logs.output)
        self.assertIsNotNone(context.exception)

    def test_validate_dim_product_valid_data(self):
        # Create a test DataFrame with valid data (no nulls or duplicates)
        test_df = pd.DataFrame({'product_id': [1, 2, 3], 'price': [10.0, 20.0, 30.0]})

        with patch('pandas.read_sql_table', return_value=test_df):
            with self.assertLogs(logger, level='INFO') as logs:
                validate_dim_product()

        # Ensure the log message indicates successful validation
        self.assertIn("Dimension table 'dim_product' validated successfully.", logs.output)

    def test_validate_dim_product_with_null_values(self):
        # Create a test DataFrame with null values
        test_df = pd.DataFrame({'product_id': [1, 2, None], 'price': [10.0, None, 30.0]})

        with patch('pandas.read_sql_table', return_value=test_df):
            with self.assertLogs(logger, level='ERROR') as logs:
                with self.assertRaises(Exception) as context:
                    validate_dim_product()

        # Ensure the log message indicates the presence of null values and an exception is raised
        self.assertIn("Null values found in the dimension table!", logs.output)
        self.assertIsNotNone(context.exception)

    # Add similar test methods for validate_dim_user and validate_dim_review
    @patch('dags.utils.dimension.pd.read_sql_table')  # Mock the read_sql_table function
    def test_validate_fact_no_null_values(self, mock_read_sql_table):
        # Create a mock DataFrame without null values
        mock_df = pd.DataFrame({
            'column1': [1, 2, 3],
            'column2': ['A', 'B', 'C']
        })

        # Set up the behavior of the mocked read_sql_table function
        mock_read_sql_table.return_value = mock_df

        # Capture log output
        with self.assertLogs(level='INFO') as logs:
            validate_fact()

        # Assertions
        self.assertTrue(mock_read_sql_table.called)
        self.assertNotIn("Error.. Null Values Found", logs.output)
        self.assertIn("fact table is validated", logs.output)

    @patch('dags.utils.dimension.pd.read_sql_table')  # Mock the read_sql_table function
    def test_validate_fact_with_null_values(self, mock_read_sql_table):
        # Create a mock DataFrame with null values
        mock_df = pd.DataFrame({
            'column1': [1, None, 3],
            'column2': ['A', 'B', None]
        })

        # Set up the behavior of the mocked read_sql_table function
        mock_read_sql_table.return_value = mock_df

        # Capture log output
        with self.assertLogs(level='ERROR') as logs:
            with self.assertRaises(Exception):
                validate_fact()

        # Assertions
        self.assertTrue(mock_read_sql_table.called)
        self.assertIn("Error.. Null Values Found", logs.output)
        self.assertNotIn("fact table is validated", logs.output)

if __name__ == '__main__':
    unittest.main()
