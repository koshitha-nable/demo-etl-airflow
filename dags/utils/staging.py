import pandas as pd
import psycopg2
import logging
from sqlalchemy import create_engine
import requests,os
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.sql.type_api import Variant
from utils.common import *


file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')

logger = logging.getLogger(__name__)

def load_user_data_to_db():
    try:

        connection = create_postgres_connection()
        cursor = connection.cursor()
        user_data = pull_user_data()
        user_data.to_sql("stg_users", con, index=False, if_exists='replace')
        logger.info("Loading user data to staging table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading user data to stg table...")

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
        logger.info("Loading product data to staging table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading product data to stg table...")

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
        logger.info("Loading transaction data to staging table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading transaction data to stg table...")

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
        logger.info("Loading review data to staging table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading review data to stg table...")

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
    logger.info("Retrieved user data")
    return user_data
    

def pull_product_data(): 
    product_data = requests.get('http://mock-api:5000/products')
    product_data = pd.DataFrame(product_data.json())
    logger.info("Retrieved product data")
    return product_data

def pull_transaction_data(): 
    transaction_data = requests.get('http://mock-api:5000/transactions')
    transaction_data = pd.DataFrame(transaction_data.json())
    logger.info("Retrieved transaction data")
    return transaction_data
