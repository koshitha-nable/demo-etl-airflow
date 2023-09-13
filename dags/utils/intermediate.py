import pandas as pd
import logging
import re
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
logger = logging.getLogger("custom_logger_int")
logger.setLevel(logging.DEBUG)

# Define the full path to the log file in the desired directory
#log_file_path = os.path.join(log_directory, "intermediate_log.log")
log_file_path = os.path.join(log_directory, "combined_log.log")

# Create a FileHandler to write logs to the specified file path
file_handler = logging.FileHandler(log_file_path)

# Set the formatter for the file handler
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

def load_user_data_to_inter():
    try:

        connection = create_postgres_connection()
        
        cursor = connection.cursor()
        query = "SELECT * FROM stg_users;"
        cursor.execute(query)
        result = cursor.fetchall()
        print(result)

        # work with the data using pandas DataFrame
        df = pd.DataFrame(result, columns=["address", "email", "name","user_id"])
        #print(df)
        
        #Remove dublicates
        df = df.drop_duplicates()

        # Remove rows with missing values
        df = df.dropna()
        print(df["user_id"])
        print(df["name"])
        print(df["address"])
        print(df["email"])

        # Validate and clean email addresses
        # df = df[df['email'].apply(lambda x: isinstance(x, str) and re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', x))]
        df = df[df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$')]
        # Remove leading/trailing whitespaces from text columns
        df['name'] = df['name'].astype(str).str.strip()
        print(df)
        #df['address'] = df['address'].str.strip()
        # Clean and transform the 'address' column
        df['address'] = df['address'].astype(str).str.replace('\n', ' ')  # Remove newline characters
        address_parts = df['address'].astype(str).str.extract(r'(.+),\s+([A-Za-z\s]+)\s+(\d+)')  # Extract city, state, and ZIP code
        df['location'] = address_parts[0]
        df['state'] = address_parts[1]
        df['zip_code'] = address_parts[2]
        df.drop(['address'], axis=1, inplace=True)
        logger.info("Transformed user data...")
        print(df)
        

        #return df
        df.to_sql("int_users", con, index=False, if_exists='replace')
        logger.info("Loading user data to intermediate table...")

        return df
        

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading user data to inter table...")
        return None

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)


def load_product_data_to_inter():
    try:

        connection = create_postgres_connection()
        
        cursor = connection.cursor()
        query = "SELECT * FROM stg_products;"
        cursor.execute(query)
        result = cursor.fetchall()
        print(result)

        # work with the data using pandas DataFrame
        df = pd.DataFrame(result, columns=["price", "product_description", "product_id","product_name"])

        # Data cleaning and transformation
        # Remove duplicate rows
        df = df.drop_duplicates()
        # Remove rows with missing values
        df = df.dropna()
        
        # Clean product descriptions (remove special characters, HTML tags, etc.)
        df['product_description'] = df['product_description'].astype(str).str.replace('[^\w\s]', '', regex=True).str.strip()
        print(df["product_description"])
        # Convert price to numeric format
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # Add a new column for discounted price
        df['discounted_price'] = df['price'] * 0.9  # Applying a 10% discount
        logger.info("Transformed product data...")
        print(df)

        con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')
        df.to_sql("int_products", con, index=False, if_exists='replace')
        logger.info("Loading product data to intermediate table...")

        return df

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        #logger.error("Error while loading product data to inter table...")
        
        return None

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)


def load_transaction_data_to_inter():
    try:

        connection = create_postgres_connection()
        
        cursor = connection.cursor()
        query = "SELECT * FROM stg_transactions;"
        cursor.execute(query)
        result = cursor.fetchall()

        # work with the data using pandas DataFrame
        df = pd.DataFrame(result, columns=["purchase_id", "product_id", "user_id","quantity"])
        # Data cleaning
        # Remove duplicate rows
        df = df.drop_duplicates()

        # Remove rows with missing values
        df = df.dropna()

        # Convert quantity to integer
        df['quantity'] = df['quantity'].astype(int)

        product_prices = pd.read_sql_table("int_products",con)  # Load product prices from the transformed products table
        df = df.merge(product_prices, on="product_id", how="left")
        df["total_amount"] = df["price"] * df["quantity"]
        df.drop(['product_name'], axis=1, inplace=True)
        df.drop(['product_description'], axis=1, inplace=True)
        df.drop(['discounted_price'], axis=1, inplace=True)
        df.drop(['price'], axis=1, inplace=True)
        logger.info("Transformed transaction data...")
        print(df)

        df.to_sql("int_transactions", con, index=False, if_exists='replace')
        logger.info("Loading transaction data to intermediate table...")

        return df

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading transaction data to inter table...")
        return None

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)


def load_review_data_to_inter():
    
    try:
        connection = create_postgres_connection()
        
        cursor = connection.cursor()
        query = "SELECT * FROM stg_reviews;"
        cursor.execute(query)
        result = cursor.fetchall()

        # work with the data using pandas DataFrame
        df = pd.DataFrame(result, columns=["review_id", "product_id", "review_score","review_date"])
        # Data cleaning
        # Remove duplicate rows
        df = df.drop_duplicates()

        # Remove rows with missing values
        df = df.dropna()
        print(df)
        # Convert review_score to integer
        df['review_score'] = df['review_score'].astype(int)

        # Convert review_date to datetime
        # Convert review_date to datetime
        df['review_date'] = pd.to_datetime(df['review_date'], format='%m/%d/%Y %H:%M')


        # Data transformation
        # Calculate the year and month of the review_date
        df['review_year'] = df['review_date'].dt.year
        df['review_month'] = df['review_date'].dt.month
        logger.info("Transformed review data...")
        print(df)
        #print(df.columns)
        df.to_sql("int_reviews", con, index=False, if_exists='replace')
        logger.info("Loading review data to intermediate table...")

        return df


    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)
        logger.error("Error while loading review data to inter table...")

        return None

    finally:
        if cursor:
            cursor.close()
        close_postgres_connection(connection)
