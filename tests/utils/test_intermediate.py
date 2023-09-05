import unittest
from unittest.mock import MagicMock, patch
from intermediate import (
    load_user_data_to_inter,
    load_product_data_to_inter,
    load_transaction_data_to_inter,
    load_review_data_to_inter,
)

class TestYourCode(unittest.TestCase):

    @patch('intermediate.create_postgres_connection')
    @patch('intermediate.close_postgres_connection')
    @patch('intermediate.pd.DataFrame.to_sql')
    def test_load_user_data_to_inter(self, mock_to_sql, mock_close_connection, mock_create_connection):
        # Mock the database connection and data retrieval
        mock_connection = MagicMock()
        mock_create_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, 'User1', 'user1@example.com', '123 Main St')]
        
        # Call the function to be tested
        load_user_data_to_inter()

        # Assertions
        mock_create_connection.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM stg_users;")
        mock_to_sql.assert_called_once()
        mock_close_connection.assert_called_once()

    @patch('intermediate.create_postgres_connection')
    @patch('intermediate.close_postgres_connection')
    @patch('intermediate.pd.DataFrame.to_sql')
    def test_load_product_data_to_inter(self, mock_to_sql, mock_close_connection, mock_create_connection):
        # Mock the database connection and data retrieval
        mock_connection = MagicMock()
        mock_create_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, 'Product1', 'Description1', 10.0)]
        
        # Call the function to be tested
        load_product_data_to_inter()

        # Assertions
        mock_create_connection.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM stg_products;")
        mock_to_sql.assert_called_once()
        mock_close_connection.assert_called_once()

    @patch('intermediate.create_postgres_connection')
    @patch('intermediate.close_postgres_connection')
    @patch('intermediate.pd.DataFrame.to_sql')
    def test_load_transaction_data_to_inter(self, mock_to_sql, mock_close_connection, mock_create_connection):
        # Mock the database connection and data retrieval
        mock_connection = MagicMock()
        mock_create_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, 1, 1, 5)]
        
        # Call the function to be tested
        load_transaction_data_to_inter()

        # Assertions
        mock_create_connection.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM stg_transactions;")
        mock_to_sql.assert_called_once()
        mock_close_connection.assert_called_once()

    @patch('intermediate.create_postgres_connection')
    @patch('intermediate.close_postgres_connection')
    @patch('intermediate.pd.DataFrame.to_sql')
    def test_load_review_data_to_inter(self, mock_to_sql, mock_close_connection, mock_create_connection):
        # Mock the database connection and data retrieval
        mock_connection = MagicMock()
        mock_create_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = [(1, 1, 5, '2023-01-01')]
        
        # Call the function to be tested
        load_review_data_to_inter()

        # Assertions
        mock_create_connection.assert_called_once()
        mock_cursor.execute.assert_called_once_with("SELECT * FROM stg_reviews;")
        mock_to_sql.assert_called_once()
        mock_close_connection.assert_called_once()

    @patch('intermediate.create_postgres_connection')
    @patch('intermediate.close_postgres_connection')
    @patch('intermediate.pd.DataFrame.to_sql')
    @patch('intermediate.logger')
    def test_data_cleaning(self, mock_logger, mock_to_sql, mock_close_connection, mock_create_connection):
        # Mock database connection and cursor
        mock_connection = MagicMock()
        mock_create_connection.return_value = mock_connection
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        # Mock the data with various cleaning scenarios
        mock_cursor.fetchall.return_value = [
            (1, 'User1', 'user1@example.com', '123 Main St'),  # Valid data
            (2, 'User2', None, '456 Elm St'),  # Missing email
            (3, 'User3', 'invalid_email', '789 Oak St'),  # Invalid email
            (4, '  User4  ', 'user4@example.com', ' \n 101 Pine St  \n '),  # Whitespace in name and address
            (5, 'User5', 'user5@example.com', 'City, State 12345'),  # Address format
        ]

        # Call the function
        load_user_data_to_inter()

        # Assertions
        mock_logger.info.assert_called_with("Loading user data to intermediate table...")


        # Verify that the data has been cleaned as expected
        mock_cursor.execute.assert_called_once_with("SELECT * FROM stg_users;")
        _, args, _ = mock_to_sql.mock_calls[0]
        _, df_args, _ = mock_to_sql.mock_calls[0]
        
        # Verify that the data has been cleaned as expected
        # Extract the DataFrame argument passed to to_sql
        df_passed_to_sql = mock_to_sql.call_args[0][1]  # Use [1] instead of [0] to access the second argument

      


if __name__ == '__main__':
    unittest.main()
