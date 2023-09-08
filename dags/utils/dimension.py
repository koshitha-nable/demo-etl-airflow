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

og_directory = "/opt/airflow/logs/custom_dag_logs"
# if not os.path.exists(log_directory):
#     os.makedirs(log_directory,exist_ok=True)

# Create a custom log formatter
log_format = "%(asctime)s [%(levelname)s] - %(message)s"
date_format = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(log_format, datefmt=date_format)

# Create a logger and set its level
logger = logging.getLogger("custom_logger_dim")
logger.setLevel(logging.DEBUG)

# Define the full path to the log file in the desired directory
log_file_path = os.path.join(log_directory, "dim_log.log")

# Create a FileHandler to write logs to the specified file path
file_handler = logging.FileHandler(log_file_path)

# Set the formatter for the file handler
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

def validate_fact():
    # Load fact table data into a DataFrame
    df = pd.read_sql_table("fact_transaction",con) 
    # Check for null values in the DataFrame
    # Raise an exception if null values are found
    if df.isnull().values.any():
        logger.error("Error.. Null Values Found")
        raise Exception("Null values found in the fact table!")
    logger.info("fact table is validated")

def validate_dim_product():
    try:
        # Load dimension table data into a DataFrame
        df = pd.read_sql_table("dim_product", con)
        
        # Check for null values in the DataFrame
        if df.isnull().values.any():
            error_message = "Null values found in the product dimension table!"
            logger.error(error_message)
            raise Exception(error_message)
        
        # Check for duplicate entries
        duplicate_rows = df[df.duplicated()]
        if not duplicate_rows.empty:
            error_message = "Duplicate entries found in the product dimension table:\n{}".format(duplicate_rows)
            logger.error(error_message)
            raise Exception(error_message)
        
        logger.info("Dimension table 'dim_product' validated successfully.")
    
    except Exception as e:
        logger.error("An error occurred during validation: %s", str(e))

def validate_dim_user():
    try:
        # Load dimension table data into a DataFrame
        df = pd.read_sql_table("dim_user", con)
        
        # Check for null values in the DataFrame
        if df.isnull().values.any():
            error_message = "Null values found in the user dimension table!"
            logger.error(error_message)
            raise Exception(error_message)
        
        # Check for duplicate entries
        duplicate_rows = df[df.duplicated()]
        if not duplicate_rows.empty:
            error_message = "Duplicate entries found in the user dimension table:\n{}".format(duplicate_rows)
            logger.error(error_message)
            raise Exception(error_message)
        
        logger.info("Dimension table 'dim_user' validated successfully.")
    
    except Exception as e:
        logger.error("An error occurred during validation: %s", str(e))


def validate_dim_review():
    try:
        # Load dimension table data into a DataFrame
        df = pd.read_sql_table("dim_review", con)
        
        # Check for null values in the DataFrame
        if df.isnull().values.any():
            error_message = "Null values found in the review dimension table!"
            logger.error(error_message)
            raise Exception(error_message)
        
        # Check for duplicate entries
        duplicate_rows = df[df.duplicated()]
        if not duplicate_rows.empty:
            error_message = "Duplicate entries found in the review dimension table:\n{}".format(duplicate_rows)
            logger.error(error_message)
            raise Exception(error_message)
        
        logger.info("Dimension table 'dim_review' validated successfully.")
    
    except Exception as e:
        logger.error("An error occurred during validation: %s", str(e))



def load_fact_transaction():
    try:

        df = pd.read_sql_table("int_transactions",con) 
        #cleaning data
        df = df.dropna()
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
        df = df.dropna(subset=['product_id'])
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
        df = df.dropna()
        df['user_id'] = df['user_id'].astype(int)
        df['zip_code'] = df['zip_code']
        df = df.dropna(subset=['user_id', 'name', 'email'])
        df = df[df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$')]
        state_mapping = {
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
            "Box": "Box",
            "WA": "Washington",
            "NJ": "New Jersey",
            "NC": "North Carolina",
            "MD": "Maryland",
            "CA": "California",
            "DE": "Delaware",
            "KY": "Kentucky",
            "TN": "Tennessee",
            "AR": "Arkansas",
            "NM": "New Mexico",
            "AR": "Arkansas",
            "KY": "Kentucky",
            "IL": "Illinois",
            "OK": "Oklahoma",
            "AZ": "Arizona",
            "CA": "California",
            "SD": "South Dakota",
            "NV": "Nevada",
            "AK": "Alaska",
            "HI": "Hawaii",
            "NY": "New York",
            "VA": "Virginia",
            "AZ": "Arizona",
            "AZ": "Arizona",
            "VT": "Vermont",
            "HI": "Hawaii",
            "NE": "Nebraska"
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


        


