from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from weekly_analysis import main

with DAG('run_weekly_news_analysis_script',
         start_date=datetime(2024,4,30),
         schedule_interval="weekly", 
         catchup=False) as dag:
    
    #create task to call the weekly analysis function
    weekly_news_analysis_task=PythonOperator(
        task_id="weekly_task",
        python_callable=main

    )

weekly_news_analysis_task



