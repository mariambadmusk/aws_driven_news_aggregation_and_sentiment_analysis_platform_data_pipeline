from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/../scripts'))
from weekly_news_etl import main
from utils import define_news_categories

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 5, 16, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
}


with DAG('run_weekly_news_analysis_script',
         default_args=default_args,
         start_date=datetime(2024,9,12),
         schedule_interval="weekly", 
         catchup=False) as dag:
    
    #create task to call the weekly analysis function
    weekly_news_analysis_task=PythonOperator(
        task_id="weekly_task",
        python_callable=main

    )

weekly_news_analysis_task



