import pandas as pd
import psycopg2
import logging
import os
from sqlalchemy import create_engine
import requests,os
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.sql.type_api import Variant
from utils.common import *


file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')

log_directory = "/opt/airflow/logs/custom_dag_logs"


# Create a custom log formatter
log_format = "%(asctime)s [%(levelname)s] - %(message)s"
date_format = "%Y-%m-%d %H:%M:%S"
formatter = logging.Formatter(log_format, datefmt=date_format)

# Create a logger and set its level
logger = logging.getLogger("custom_logger_stg")
logger.setLevel(logging.DEBUG)

# Define the full path to the log file in the desired directory
#log_file_path = os.path.join(log_directory, "staging_log.log")
log_file_path = os.path.join(log_directory, "combined_log.log")

# Create a FileHandler to write logs to the specified file path
file_handler = logging.FileHandler(log_file_path)

# Set the formatter for the file handler
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

def load_data_to_db(api_endpoint, staging_table_name):
    try:
        connection = create_postgres_connection()
        cursor = connection.cursor()
        data = pull_api_data(api_endpoint)
        data.to_sql(staging_table_name, con, index=False, if_exists='replace')
        logger.info(f"Loading {api_endpoint} data to {staging_table_name} table...")
        return True

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error(f"Error while loading {api_endpoint} data to {staging_table_name} table...")
        return False

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)


def load_review_to_db():
    try:
        
        connection =create_postgres_connection()
        cursor = connection.cursor()
        review_data = get_reviews()
        review_data.to_sql("stg_reviews", con, index=False, if_exists='replace')
        logger.info("Loading review data to staging table...")
        return True

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading review data to stg table...")
        return False

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)


def get_reviews():

    try: 
        
        file_path = os.path.join(file_root,'src_data/reviews.csv')

        if not file_path.exists():
            logger.error(f"Reviews file not found at {file_path}")
            return None
        
        reviews_data = pd.read_csv(file_path)
        logger.info("Retrieved review data")
        return reviews_data
    
    except Exception as error:
        logger.error(f"Error while reading review data: {error}")
        return None
    

def pull_api_data(api_endpoint):
    try:
        data = requests.get(f'http://mock-api:5000/{api_endpoint}')
        data = pd.DataFrame(data.json())
        logger.info(f"Retrieved {api_endpoint} data")
        return data
    except Exception as error:
        logger.error(f"Error while retrieving {api_endpoint} data: {error}")
        return None