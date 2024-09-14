from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import configparser
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../scripts'))
import logging
from logging_config import setup_logging

# set up logging
setup_logging()

def get_database_connection():
    try:
        connection = get_database_connection()
        logging.info("Database connection established successfully to remove duplicates.")
        cursor = connection.cursor()
        return connection, cursor
    except Exception as e:
        logging.error(f"Failed to connect to the database to remove duplicates: {e}")
        return None


def perform_duplicate_check():
    connection, cursor = get_database_connection()
    connection.autocommit = True

    if not connection:
        return None
    
    categories = {'business', 'crime', 'education', 'entertainment', 'health', 'science'}
    try:
        for category in categories:
                    table_name = category
                    delete_query = f"""
                        DELETE FROM {table_name}
                        WHERE (article_id) IN (
                            SELECT article_id, pubDate
                            FROM (
                                SELECT article_id, pubDate,
                                    ROW_NUMBER() OVER (PARTITION BY article_id ORDER BY pubDate) AS row_num
                                FROM {category}
                            ) AS duplicates
                            WHERE row_num > 1
                        );
                    """
                    cursor.execute(delete_query)
                
                    print("Duplicate check completed successfully.")
    except Exception as e:
        print(f'An error occurred during duplicate check: {e}')
    finally:
        if connection:
            connection.close()

def main():
    perform_duplicate_check()

if __name__ == "__main__":
    main()


# Define the DAG
with DAG('remove duplicates',
         start_time=datetime(2024, 9, 12, 18, 0, 0),
         schedule_interval='@daily',
         )as dag:
    
    perform_duplicate_check_task = PythonOperator(
        task_id='perform_duplicate_check',
        python_callable=main,

    )


    

