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
        #cleaning data
        df = df.dropna(subset=['purchase_id'])
        df = df.drop_duplicates(subset=['purchase_id'])
        #casting
        df['purchase_id'] = df['purchase_id'].astype(int)
        df['product_id'] = df['product_id'].astype(int)
        df['user_id'] = df['user_id'].astype(int)
        df['quantity'] = df['quantity'].astype(int)
        df['total_amount'] = df['total_amount'].astype(float)
        #Remove rows with very high quantities or amounts
        df = df[(df['quantity'] > 0) & (df['total_amount'] > 0)]

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
        df = df.dropna(subset=['purchase_id'])
        df['product_id'] = df['product_id'].astype(int)
        df['price'] = df['price'].astype(float)
        df['discounted_price'] = df['discounted_price'].astype(float)
        df['product_description'].fillna('', inplace=True)
        df['product_name'] = df['product_name'].str.title()

        df.to_sql("dim_product",con, index=False, if_exists='replace')
        logger.info("Loading product data to dim table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading product data to dim table...")


def load_dim_user():
    try:

        df = pd.read_sql_table("int_users",con) 
        df.drop_duplicates(subset=['user_id'], keep='first', inplace=True)
        df['user_id'] = df['user_id'].astype(int)
        df['zip_code'] = df['zip_code'].astype(int)
        df = df.dropna(subset=['user_id', 'name', 'email'])
        df = df[df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$')]
        state_mapping = state_dictionary = {
            "FL": "Florida",
            "AL": "Alabama",
            "RI": "Rhode Island",
            "AK": "Alaska",
            "AL": "Alabama",
            "NY": "New York",
            "CT": "Connecticut",
            "AR": "Arkansas",
            "MS": "Mississippi",
            "HI": "Hawaii",
            "CA": "California",
            "NV": "Nevada",
            "OH": "Ohio",
            "KS": "Kansas",
            "ID": "Idaho",
            "GA": "Georgia",
            "CO": "Colorado",
            "WY": "Wyoming",
            "LA": "Louisiana",
            "WI": "Wisconsin",
            "Box":"Box",
            "WA": "Washington",
            "NJ": "New Jersey"
        }
        df['state'] = df['state'].map(state_mapping)

        df.to_sql("dim_user",con, index=False, if_exists='replace')
        logger.info("Loading user data to dim table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading user data to dim table...")


def load_dim_review():
    try:

        df = pd.read_sql_table("int_reviews",con) 
        df.drop_duplicates(subset=['review_id'], keep='first', inplace=True)
        df = df.dropna(subset=['review_id'])
        df['product_id'] = df['product_id'].astype(int)
        df['review_score'] = df['review_score'].astype(int)
        df['review_date'] = pd.to_datetime(df['review_date'])

        df.to_sql("dim_review",con, index=False, if_exists='replace')
        logger.info("Loading review data to dim table...")

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading review data to dim table...")


        


