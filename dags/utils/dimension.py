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

def load_fact_transaction():
    try:

        df = pd.read_sql_table("int_transactions",con) 
        df.to_sql("fact_transaction",con, index=False, if_exists='append')

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)


def load_dim_product():
    try:

        df = pd.read_sql_table("int_products",con) 
        df.to_sql("dim_product",con, index=False, if_exists='append')

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)


def load_dim_user():
    try:

        df = pd.read_sql_table("int_users",con) 
        df.to_sql("dim_user",con, index=False, if_exists='append')

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)


def load_dim_review():
    try:

        df = pd.read_sql_table("int_reviews",con) 
        df.to_sql("dim_review",con, index=False, if_exists='append')

    except Exception as error:
        print("Error while connecting to PostgreSQL:", error)


        


