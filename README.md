### Data Pipeline for News Sentiment Analysis (Daily & Weekly)

#### Description:
    This project automates a data pipeline to collect daily news articles from Newsdata API, perform sentiment analysis, and store the processed data in PostgreSQL deployed to Amazon RDS

#### Key Features:
    - Daily Data Collection:
        - Extracts news articles (title, description, source, link, published date, country) using Newsdata API.
        - Schedules data collection for each category every 30 minutes to stay within API rate limits.
    - Sentiment Analysis:
        - Calculates article length and description length.
        - Determines sentiment based on article descriptions (implementation details can be provided in the code or a separate document).
        - Using Hugging Face Transformers library  nlptown/bert-base-multilingual-uncased-sentiment
- Data Storage:
        - Stores scraped data in a PostgreSQL database.
        - Connects PostgreSQL to Amazon RDS for persistent storage.
        - Transfers data to Amazon Redshift for efficient querying and analysis.

- Weekly Aggregation:
    - Runs once a week (specify day and time).
    - Reads the last 7 days of news data from PostgreSQL.
    - Calculates mean word count, mean article length, and most popular sentiment for each category.
    - Stores aggregated data in a separate weekly database within PostgreSQL.

#### Technology Stack:
    - Programming Language: Python 3.11+
    - Cloud Infrastructure: EC2 Instance
    - Data Orchestration: Airflow DAGs, AWS Lambda
    - Database: PostgreSQL (connected to Amazon RDS)
    - API: Newsdata API  - newsdata.io

#### Prerequisites:
    - Python 3.8+ with pip installed
    - Airflow DAG configuration
    - Access to these AWS services: Amazon Elastic Compute, Amazon Redshift and Amazon RDS, AWS Identity and management
    - PostgreSQL client library

#### How It Works:
    The project has two Airflow DAGs:
    - Daily News DAG:
        - Runs the daily_news_etl_job.py script daily at 11:00am .
        - Schedules each news category to run every 30 minutes for efficient API usage.
        - Scrapes data from the API, processes it (including sentiment analysis), and inserts it into the PostgreSQL daily news database.

    - Weekly News DAG:
        - Runs the weekly_news_etl_job.py once a week at 13:35PM.
        - Executes a Python script to read the last 7 days of news data from PostgreSQL.
        - Performs aggregations (mean word count, mean article length, most popular sentiment).
        - Stores aggregated data in the PostgreSQL weekly news database.

#### Data Flow:
    - Data is extracted with the Newsdata API.
    - Data is processed (including sentiment analysis).
    - Processed data is loaded in PostgreSQL.
    - PostgreSQL connects to Amazon RDS for scalability.
.
    
#### Getting Started (Optional):
    - Set up an Amazon EC2 instance - Linux
    - Clone Repository
    - Configure your environment (Python, Airflow, AWS credentials, Postgresql).
    - Install dependencies using pip install -r requirements.txt.
    - Update Airflow DAGs with your desired parameters (schedule, database connection details).
    - Configure AWS Lambda to start EC2 instance at schedule

#### Challenges:
    - API 
        - Using https://newsdata.io/ free tier provides 200 API credit at one article per 10 credit which restricted the numbers of articles that can be requested to a maximum of 20 articles per day.
        - A request limit is 10 articles per page for the free tier.
        - Option for the full content of news not available in the free tier
    - Data Quality 
        - Request did not limit news sources to certain websites or news sites. It is open ended.

#### Error Handling
#### Scalability
#### Testing 

#### Additional Notes:
    - Newsdata API Documentation: https://newsdata.io/documentation 
    - Hugging Face Transformers Documentation: https://huggingface.co/docs/transformers/en/index






