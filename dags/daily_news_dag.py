from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from collections import OrderedDict
from airflow.operators.dummy import DummyOperator
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../scripts'))
from daily_news_etl import instantiate_model, crawl_and_process_categories
from utils import define_news_categories



# Default arguments for the DAG
default_args = {
    'owner': 'kike',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 12, 9, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id="daily_news_dag",
    default_args=default_args,
    schedule_interval=timedelta(minutes=30),  # Schedule DAG to run every 30 minutes
    catchup=False,  
    max_active_runs=1, 
    ) as dag:
    
    # Create a dummy start task
    start_task = DummyOperator(task_id='start_task')

    # Instantiate the model task
    instantiate_model_task = PythonOperator(
        task_id="instantiate_model",
        python_callable=instantiate_model,
        dag=dag
    )

    # Define the categories using the function from utils
    category_list = define_news_categories()

    # List to hold dynamically created tasks
    crawl_and_process_category_tasks = []

    # Create tasks for each category and set them to run every 30 minutes
    for category in category_list:
        task = PythonOperator(
            task_id=f'crawl_and_process_{category}',  
            python_callable=crawl_and_process_categories,  
            op_args=[category],  
            dag=dag
        )
        crawl_and_process_category_tasks.append(task)

        # Define task dependencies
        instantiate_model_task >> task  

    # Set start_task to trigger instantiate_model_task
    start_task >> instantiate_model_task
    # Create a dummy end task
    end_task = DummyOperator(task_id='end_task')

    # Set task dependencies
    for task in crawl_and_process_category_tasks:
        task >> end_task  

    # Set end_task to trigger start_task for continuous execution
    end_task >> start_task
