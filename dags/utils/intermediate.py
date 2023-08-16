import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import requests,os
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.sql.type_api import Variant

file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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
        
        #Remove dublicates
        df = df.drop_duplicates()

        # Remove rows with missing values
        df = df.dropna()

        # Validate and clean email addresses
        df = df[df['email'].str.contains(r'^[\w\.-]+@[\w\.-]+\.\w+$')]

        # Remove leading/trailing whitespaces from text columns
        df['name'] = df['name'].str.strip()
        #df['address'] = df['address'].str.strip()

        # Clean and transform the 'address' column
        df['address'] = df['address'].str.replace('\n', ' ')  # Remove newline characters
        address_parts = df['address'].str.extract(r'(.+),\s+([A-Za-z\s]+)\s+(\d+)')  # Extract city, state, and ZIP code
        df['location'] = address_parts[0]
        df['state'] = address_parts[1]
        df['zip_code'] = address_parts[2]
        df.drop(['address'], axis=1, inplace=True)
        print(df)
        print(df.shape[1])
        print(df.columns)



        #return df
        con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')
        df.to_sql("int_users", con, index=False, if_exists='append')

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

        # Data cleaning and transformation
        # Remove duplicate rows
        df = df.drop_duplicates()

        # Remove rows with missing values
        df = df.dropna()

        # Clean product descriptions (remove special characters, HTML tags, etc.)
        df['product_description'] = df['product_description'].str.replace('[^\w\s]', '').str.strip()

        # Convert price to numeric format
        df['price'] = pd.to_numeric(df['price'], errors='coerce')

        # Add a new column for discounted price
        df['discounted_price'] = df['price'] * 0.9  # Applying a 10% discount

        con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')
        df.to_sql("int_products", con, index=False, if_exists='append')


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
        # Data cleaning
        # Remove duplicate rows
        df = df.drop_duplicates()

        # Remove rows with missing values
        df = df.dropna()

        # Convert quantity to integer
        df['quantity'] = df['quantity'].astype(int)

        con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')
        product_prices = pd.read_sql_table("int_products", con)  # Load product prices from the transformed products table
        df = df.merge(product_prices, on="product_id", how="left")
        df["total_amount"] = df["price"] * df["quantity"]
        df.drop(['product_name'], axis=1, inplace=True)
        df.drop(['product_description'], axis=1, inplace=True)
        df.drop(['discounted_price'], axis=1, inplace=True)
        df.drop(['price'], axis=1, inplace=True)
        print(df.columns)
        
       # df.to_sql("int_transactions", con, index=False, if_exists='append')


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
                # Data cleaning
        # Remove duplicate rows
        df = df.drop_duplicates()

        # Convert review_score to integer
        df['review_score'] = df['review_score'].astype(int)

        # Convert review_date to datetime
        df['review_date'] = pd.to_datetime(df['review_date'], format='%Y-%m-%d')

        # Data transformation
        # Calculate the year and month of the review_date
        df['review_year'] = df['review_date'].dt.year
        df['review_month'] = df['review_date'].dt.month


        con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')
        print(df.columns)
        #df.to_sql("int_reviews", con, index=False, if_exists='append')


    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

