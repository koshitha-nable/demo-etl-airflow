import unittest
from unittest.mock import patch
import pandas as pd

import sys,os

# Add the parent directory of your project to the Python path
project_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_path)

from dags.utils.staging import (
   pull_api_data,
   get_reviews,
   load_data_to_db,
   load_review_to_db
   
)


class TestStgCode(unittest.TestCase):

    
    def test_pull_api_data_success(self):
        
        # Calling the function with a sample API endpoint
        result_user = pull_api_data("users")
        result_product = pull_api_data("products")
        result_transactions = pull_api_data("transactions")

        # Assertions
        self.assertIsNotNone(result_user)  # Ensure the result is not None
        self.assertIsInstance(result_user, pd.DataFrame)  # Ensure the result is a DataFrame
        self.assertEqual(len(result_user),30)

        self.assertIsNotNone(result_product)  # Ensure the result is not None
        self.assertIsInstance(result_product, pd.DataFrame)  # Ensure the result is a DataFrame
        self.assertEqual(len(result_product),50)

        self.assertIsNotNone(result_transactions)  # Ensure the result is not None
        self.assertIsInstance(result_transactions, pd.DataFrame)  # Ensure the result is a DataFrame
        self.assertEqual(len(result_transactions),300)


    def test_pull_api_data_fail(self):
        
        # Calling the function with a sample API endpoint
        result_user = pull_api_data("user")
        result_user1 = pull_api_data("users")
        result_product = pull_api_data("product")
        result_product1 = pull_api_data("products")
        result_transactions = pull_api_data("transaction")
        result_transactions1 = pull_api_data("transactions")

        # Assertions
        self.assertIsNone(result_user)  # Ensure the result is not None
        self.assertNotEqual(len(result_user1),31)

        self.assertIsNone(result_product)  # Ensure the result is not None  
        self.assertNotEqual(len(result_product1),31)

        self.assertIsNone(result_transactions)  # Ensure the result is not None
        self.assertNotEqual(len(result_transactions1),40)

    
    @patch('dags.utils.staging.os.path.exists')
    def test_get_reviews_file_exists(self,  mock_exists):
        # Mock the file exists check to return True
        mock_exists.return_value = True

        # Call the function
        result = get_reviews()

        # Assert that the function returns the expected result
        self.assertIsNotNone(result) 
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 135)
        self.assertEqual(result.shape, (135, 4))
        self.assertNotEqual(len(result), 136)
        

    @patch('dags.utils.staging.os.path.exists')
    def test_get_reviews_file_not_found(self, mock_exists):
        # Mock the file exists check to return False
        mock_exists.return_value = False

        # Call the function
        result = get_reviews()

        # Assert that the function returns None when the file is not found
        self.assertIsNone(result)

    def test_load_data(self):

        result_user = load_data_to_db('users', 'stg_users')
        result_user1 = load_data_to_db('user', 'stg_users')
        self.assertIsNotNone(result_user) 
        self.assertIsNone(result_user1)
        
        result_products = load_data_to_db('products', 'stg_products')
        result_products1 = load_data_to_db('product', 'stg_products')
        self.assertIsNotNone(result_products) 
        self.assertIsNone(result_products1)

        result_transactions = load_data_to_db('transactions', 'stg_transactions')
        result_transactions1 = load_data_to_db('transaction', 'stg_transactions')
        self.assertIsNotNone(result_transactions) 
        self.assertIsNone(result_transactions1)


    def test_load_review_success(self):
        
        result = load_review_to_db()

        # Assert that the function returns the expected result
        self.assertIsNotNone(result) 
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape, (135, 4))
    
if __name__ == '__main__':
    unittest.main()
