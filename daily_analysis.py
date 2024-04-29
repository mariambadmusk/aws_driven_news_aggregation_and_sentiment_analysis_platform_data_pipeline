from bs4 import BeautifulSoup
import requests 
from newsdataapi import NewsDataApiClient
import schedule
import time
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import numpy as np
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from collections import Counter
from datetime import datetime

# Instantiate tokenizer and model for sentiment analysis
def instantiate_model():
    tokenizer = AutoTokenizer.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')
    model = AutoModelForSequenceClassification.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment', num_labels=3)
    return tokenizer, model

# Function to fetch data from News API based on the provided query
def get_api(query):
    try:
        api = NewsDataApiClient(apikey='pub_39058ad28ae246ef9dd6ef5cbddd0efecc7ad')
        response = api.news_api(
                            q=query,
                            language='en',
                            image=False,
                            video=False,
                            size=30
                            )
        print(f'News Api get request: {query} success')
        return response
    except Exception as e:
        print(f'News Api get request failed: {e}')
        return None

# Function to create DataFrame from the fetched API response
def create_dataframe(query):
    try:
        response = get_api(query)
        if response:
            df = pd.DataFrame(response['results'], columns=["article_id", "title", "description", "source", "link", "pubDate", "country"])
            return df
        else:
            return None
    except Exception as e:
        print(f'Dataframe transformation failed: {e}')
        return None 

# Function to run sentiment analysis and add sentiment labels to DataFrame
def add_sentiment_labels(df, tokenizer, model):
    try:
        df['sentiment'] = df['description'].apply(lambda x: sentiment_label(x, tokenizer, model))
        return df
    except Exception as e:
        print(f'Sentiment analysis failed: {e}')
        return None

# Function to perform sentiment analysis on provided news text
def sentiment_label(news, tokenizer, model):
    with torch.no_grad():
        tokens = tokenizer.encode(news, return_tensors='pt', padding=True, truncation=True)
        results = model(tokens)
    predicted_class = torch.argmax(results.logits, dim=1).item()
    
    if predicted_class == 0:
        sentiment = "negative"
    elif predicted_class == 1:
        sentiment = "neutral"
    else:
       sentiment = "positive"
    return sentiment

# Function to insert DataFrame into SQL database
def insert_to_sql_db(df, table_name, connection):
    try:
        df.to_sql(table_name, connection, if_exists='append', index=False)
        print(f'{table_name} appended to database successfully')
    except Exception as e:
        print(f'Failed to append {table_name} to database: {e}')

# Main function to run API calls, sentiment analysis, and database insertion for each category
def crawl_and_process_categories():
    try:
        # Connect to the database
        db_params = {
            'database': 'news_sentiment_analysis_db',
            'host': 'localhost',
            'user': 'postgres',
            'password': '5432'
        }
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()
        connection.set_session(isolation_level='READ COMMITTED', readonly=False, deferrable=False, autocommit=True)

        # Instantiate tokenizer and model
        tokenizer, model = instantiate_model()

        # Categories to crawl
        categories = ['business', 'crime', 'education', 'entertainment', 'health', 'science']

        for category in categories:
            print(f'Crawling and processing {category} data...')
            df = create_dataframe(category)
            if df is not None:
                df = add_sentiment_labels(df, tokenizer, model)
                if df is not None:
                    df.insert(0, 'category_id', category_id_mapping[category])
                    insert_to_sql_db(df, category, connection)
            else:
                print(f'Failed to fetch data for category: {category}')
        
        connection.close()
        print("All categories crawled, processed, and inserted into the database.")
    except Exception as e:
        print(f'Error occurred: {e}')

# Mapping of category names to category IDs
category_id_mapping = {
    'business': 101,
    'crime': 102,
    'education': 103,
    'entertainment': 104,
    'health': 105,
    'science': 106
}

# Schedule daily execution of the main function for each category
for category in category_id_mapping.keys():
    schedule.every(30).minutes.do(crawl_and_process_category, category)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(60)  # Check every minute


def schedule_daily_crawl_and_process(category):
      