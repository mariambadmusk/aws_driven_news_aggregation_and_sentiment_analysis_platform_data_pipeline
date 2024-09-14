import psycopg2
import logging
from logging_config import setup_logging
from psycopg2 import sql
import configparser
import os

# set up logging
setup_logging()

config = configparser.ConfigParser()
config.read('.env')

postgres_db = config['POSTGRES']['POSTGRES_DB']
postgres_user = config['POSTGRES']['POSTGRES_USER']
postgres_password = config['POSTGRES']['POSTGRES_PASSWORD']
postgres_host = config['POSTGRES']['POSTGRES_HOST']
postgres_port = config['POSTGRES']['PORT']


# map categories and category_id
def define_news_categories():
    category_id_mapping = {
                'business': 101,
                'crime': 102,
                'education': 103,
                'entertainment': 104,
                'health': 105,
                'science': 106
            }
    return category_id_mapping


# get_database_connection function
def get_database_connection():
    try:
     
        db_config = {
            'dbname': postgres_db,
            'user': postgres_user,
            'password': postgres_password,
            'host': postgres_host,
            'port': postgres_port
        }
    
        # db_config = {
        #     'dbname': os.getenv('POSTGRES_DB'),
        #     'user': os.getenv('POSTGRES_USER'),
        #     'password': os.getenv('POSTGRES_PASSWORD'),
        #     'host': os.getenv('POSTGRES_HOST'),
        #     'port': os.getenv('POSTGRES_PORT')
        # }
        # Create a connection to the PostgreSQL database
        connection = psycopg2.connect(**db_config)
        logging.info("Database connection established successfully.")
        return connection
    except psycopg2.DatabaseError as error:
        logging.error(f"Error connecting to the database: {error}")
        return None


if __name__ == "__main__":
    get_database_connection()

