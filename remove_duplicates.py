from datetime import datetime, timedelta
import psycopg2
from psycopg2 import sql
import configparser
from airflow import DAG
from airflow.operators.python import PythonOperator


def perform_duplicate_check():

    # Connect to the database
    config = configparser.ConfigParser()
    config.read('.env')

    db_params = {
        'database': 'news_sentiment_analysis_db',
        'host': 'localhost',
        'user': 'postgres',
        'password': config['POSTGRES']['password']
    }

    try:
        categories= {'business', 'crime','education','entertainment', 'health', 'science'}

        # Connect to the database
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        connection.autocommit = True

        
        
        # Perform duplicate check 
        for category in categories:
            table_name = category
            check_query = f"""
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
            cursor.execute(check_query)
            connection.commit()
        
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


#

with DAG('remove duplicates',
         start_time=datetime(2024, 4,30, 3, 0, 0),
         schedule_interval='@daily'
         )as dag:
    
    perform_duplicate_check_task = PythonOperator(
        task_id='perform_duplicate_check',
        python_callable=main

    )


    

