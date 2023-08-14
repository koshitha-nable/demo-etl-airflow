import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import requests
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.sql.type_api import Variant

def load_user_data_to_inter():
    try:

        connection = psycopg2.connect(
            user=Variable.get("POSTGRES_USER"),
            password=Variable.get("POSTGRES_PASSWORD"),
            host="remote_db",
            database=Variable.get("DB_NAME")
        )
        
        cursor = connection.cursor()
        query = "SELECT * FROM stg_users;"
        cursor.execute(query)
        result = cursor.fetchall()

        # work with the data using pandas DataFrame
        df = pd.DataFrame(result, columns=["user_id", "name", "email","address"])
        return df

        

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def load_product_data_to_inter():
    try:

        connection = psycopg2.connect(
            user=Variable.get("POSTGRES_USER"),
            password=Variable.get("POSTGRES_PASSWORD"),
            host="remote_db",
            database=Variable.get("DB_NAME")
        )
        
        cursor = connection.cursor()
        query = "SELECT * FROM stg_products;"
        cursor.execute(query)
        result = cursor.fetchall()

        # work with the data using pandas DataFrame
        df = pd.DataFrame(result, columns=["product_id", "product_name", "product_description","price"])
        return df


    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def load_transaction_data_to_inter():
    try:

        connection = psycopg2.connect(
            user=Variable.get("POSTGRES_USER"),
            password=Variable.get("POSTGRES_PASSWORD"),
            host="remote_db",
            database=Variable.get("DB_NAME")
        )
        
        cursor = connection.cursor()
        query = "SELECT * FROM stg_transactions;"
        cursor.execute(query)
        result = cursor.fetchall()

        # work with the data using pandas DataFrame
        df = pd.DataFrame(result, columns=["purchase_id", "product_id", "user_id","quantity"])
        return df


    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def load_review_data_to_inter():
    try:

        connection = psycopg2.connect(
            user=Variable.get("POSTGRES_USER"),
            password=Variable.get("POSTGRES_PASSWORD"),
            host="remote_db",
            database=Variable.get("DB_NAME")
        )
        
        cursor = connection.cursor()
        query = "SELECT * FROM stg_reviews;"
        cursor.execute(query)
        result = cursor.fetchall()

        # work with the data using pandas DataFrame
        df = pd.DataFrame(result, columns=["review_id", "product_id", "review_score","review_date"])
        return df


    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

