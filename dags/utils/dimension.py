import logging
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

# # Create and configure logger
logging.basicConfig(filename="newfile.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')
 
# Creating an object
#logger = logging.getLogger()
 
logger = logging.getLogger(__name__)

def validate_null_values():
    # Load fact table data into a DataFrame
    df = pd.read_sql_table("fact_transaction",con) 
    # Check for null values in the DataFrame
    # Raise an exception if null values are found
    if df.isnull().values.any():
        logger.error("Error.. Null Values Found")
        raise Exception("Null values found in the fact table!")
    logger.info("Validated")
        

def load_fact_transaction():
    try:

        df = pd.read_sql_table("int_transactions",con) 
        df.to_sql("fact_transaction",con, index=False, if_exists='replace')
        logger.info("Loading transaction data to fact table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading transaction data to dim table...")


def load_dim_product():
    try:

        df = pd.read_sql_table("int_products",con) 
        # Drop duplicates based on 'user_id'
        df.drop_duplicates(subset=['product_id'], keep='first', inplace=True)
        df.to_sql("dim_product",con, index=False, if_exists='replace')
        logger.info("Loading product data to dim table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading product data to dim table...")


def load_dim_user():
    try:

        df = pd.read_sql_table("int_users",con) 
        df.drop_duplicates(subset=['user_id'], keep='first', inplace=True)
        df.to_sql("dim_user",con, index=False, if_exists='replace')
        logger.info("Loading user data to dim table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading user data to dim table...")


def load_dim_review():
    try:

        df = pd.read_sql_table("int_reviews",con) 
        df.drop_duplicates(subset=['review_id'], keep='first', inplace=True)
        df.to_sql("dim_review",con, index=False, if_exists='replace')
        logger.info("Loading review data to dim table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading review data to dim table...")


        


