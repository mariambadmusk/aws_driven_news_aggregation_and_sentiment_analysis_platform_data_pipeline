from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from collections import OrderedDict
from daily_analysis import instantiate_model, main


category_id_mapping = {
                    'business': 101,
                    'crime': 102,
                    'education': 103,
                    'entertainment': 104,
                    'health': 105,
                    'science': 106
                }


#define dag for fethcing news daily 
with DAG("daily_news_dag", 
         start_date=datetime(2024,4,30,11,0,0),
         schedule_interval="@daily", 
         catchup=False) as dag:
    
    #Instantiate model task
    instantiate_model= PythonOperator(
        task_id= "instantiate_model",
        python_callable=instantiate_model
    )



#schedule each category crawl and process ofr every 30 miutes
    for category in category_id_mapping.keys():
        #schedule time for each category
        dag_id = f"crawl_and_process_{category}"
        dag = DAG(dag_id = dag_id, #dynamic dag name for each category
            schedule_interval=timedelta(minutes=30), #run every 30 minutes for each category
            start_date=datetime(2024,4,30,11,0,1)
        )

        #create task to call each crawl_and_process_categories
        crawl_and_process_catgory_task= PythonOperator(
            task_id='crawl_and_process',
            python_callable=main,
            op_args=[category],
            dag=dag,
        )


#define task dependencies
instantiate_model >> crawl_and_process_catgory_task





            
            
            
            
            