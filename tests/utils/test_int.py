import unittest
import sys
import pandas as pd
from unittest.mock import MagicMock, patch

# Add the path to the 'dags' directory to the Python path
sys.path.append('/opt/airflow/dags')

# Now you can import your functions and classes
from dags.utils.intermediate import (
    pull_user_data,
    pull_product_data,
    pull_transaction_data,
    load_user_data_to_db,
    load_product_data_to_db,
    load_transaction_data_to_db,
    get_reviews,
)

class TestYourCode(unittest.TestCase):
    
    @patch('requests.get')
    def test_pull_user_data(self, mock_get):
        # Mock the requests.get function to return sample data
        mock_response = MagicMock()
        mock_response.json.return_value = [{'id': 1, 'name': 'User1'}, {'id': 2, 'name': 'User2'}]
        mock_get.return_value = mock_response
        
        user_data = pull_user_data()
        
        # Ensure that the function returns a DataFrame with expected data
        expected_data = pd.DataFrame([{'id': 1, 'name': 'User1'}, {'id': 2, 'name': 'User2'}])
        pd.testing.assert_frame_equal(user_data, expected_data)

    # Similar tests can be written for pull_product_data and pull_transaction_data functions.

    @patch('pandas.DataFrame.to_sql')
    def test_load_user_data_to_db(self, mock_to_sql):
        # Mock the pd.DataFrame.to_sql function to ensure it is called correctly
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_to_sql.return_value = None
        
        with patch('dags.utils.intermediate.create_postgres_connection', return_value=mock_connection):
            with patch('dags.utils.intermediate.pull_user_data', return_value=pd.DataFrame()):
                load_user_data_to_db()
        
        # Ensure that the to_sql function is called with the correct arguments
        mock_to_sql.assert_called_once_with('stg_users', mock_connection, index=False, if_exists='replace')

    # Similar tests can be written for load_product_data_to_db, load_transaction_data_to_db, and load_review_to_db functions.

    def test_get_reviews(self):
        review_data = get_reviews()
        
        # Ensure that the function returns a DataFrame with expected data
        expected_data = pd.DataFrame([{'id': 1, 'text': 'Review1'}, {'id': 2, 'text': 'Review2'}])
        pd.testing.assert_frame_equal(review_data, expected_data)

if __name__ == '__main__':
    unittest.main()
