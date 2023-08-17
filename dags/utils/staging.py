import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import requests,os
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.sql.type_api import Variant
from utils.common import *

file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')

def load_user_data_to_db():
    try:

        connection = create_postgres_connection()
        cursor = connection.cursor()
        user_data = pull_user_data()
        user_data.to_sql("stg_users", con, index=False, if_exists='replace')

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)


def load_product_data_to_db():
    try:
        
        connection = create_postgres_connection()
        cursor = connection.cursor()
        product_data = pull_product_data()
        product_data.to_sql("stg_products", con, index=False, if_exists='replace')

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)


def load_transaction_data_to_db():
    try:
        
        connection = create_postgres_connection()
        cursor = connection.cursor()
        transaction_data = pull_transaction_data()
        transaction_data.to_sql("stg_transactions", con, index=False, if_exists='replace')

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)

def load_review_to_db():
    try:
        
        connection =create_postgres_connection()
        cursor = connection.cursor()
        review_data = get_reviews()
        review_data.to_sql("stg_reviews", con, index=False, if_exists='append')

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)



def get_reviews():
    transaction_data = pd.read_csv(os.path.join(file_root,'src_data/reviews.csv'))
    return transaction_data


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
