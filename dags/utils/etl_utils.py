import pandas as pd
import requests, os
import numpy as np
import psycopg2
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.sql.type_api import Variant

file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def pull_user_data(): 
    user_data = requests.get('http://mock-api:5000/users')
    user_data = pd.DataFrame(user_data.json())
    return user_data

def pull_product_data(): 
    product_data = requests.get('http://mock-api:5000/products')
    product_data = pd.DataFrame(product_data.json())
    return product_data

def pull_transaction_data(): 
    transaction_data = requests.get('http://mock-api:5000/transactions')
    transaction_data = pd.DataFrame(transaction_data.json())
    return transaction_data

print(file_root)
