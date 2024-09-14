#import necessary dependecies
from bs4 import BeautifulSoup
from datetime import datetime
import requests 
from newsdataapi import NewsDataApiClient
import time
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import numpy as np
from collections import Counter
import configparser
import logging
from utils import define_news_categories, get_database_connection
from logging_config import setup_logging

# set up logging
setup_logging()


config = configparser.ConfigParser()
config.read('.env')


#initialise tokenizer and model for sentiment analysis
def instantiate_model():
    try:
        tokenizer = AutoTokenizer.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment')
        model = AutoModelForSequenceClassification.from_pretrained('nlptown/bert-base-multilingual-uncased-sentiment', num_labels=3)
        return tokenizer, model
    except Exception as e:
        logging.error(f"Error initializing sentiment analysis model: {e}")
        raise RuntimeError(f"Error initialising sentiment analysis model: {e}")


#get news data from newsdataapi for a category
def get_api(category):
    try:
        api_key = config['NEWSAPI']['apikey']
        url = f"https://newsdataapi.com/api/v1/news?apikey={api_key}&q={category}&language=en&image=false&video=false&size=30"
        response = requests.get(url).json()
        logging.info(f'News API get request: {category} success')
        print(f'News Api get request: {category} sucesss')
        return response
    except Exception as e:
        logging.error(f'News API get request failed: {e}')
        return None


#create dataframe from the api response for a category
def create_dataframe(response):
    try:
        if response:
            df = pd.DataFrame(response['results'], columns = ["article_id", "title", "description", "source", "link", "pubDate", "country"])
            return df
        else:
            return None
    except Exception as e:
        logging.error(f'Failed to create dataframe: {e}')
        return None 

#addcolumns for wordCount and articleLength to dataframe
def add_count_sentiment(df):
    df['wordCount'] = df['description'].apply(lambda x: len(x.split()))
    df['articleLength'] = df['description'].apply(lambda x: len(x)) 
    return df

#perform sentiment analysis on news using the tokeniser and model
def perform_sentiment_analysis(text, tokenizer, model):
    if not tokenizer or not model:
        return None
    with torch.no_grad():
        tokens = tokenizer.encode(text, return_tensors='pt', padding=True, truncation=True)
        results = model(tokens)
    predicted_class = torch.argmax(results.logits, dim=1).item()
    
    if predicted_class == 0:
        sentiment = "negative"
    elif predicted_class == 1:
        sentiment = "neutral"
    else:
       sentiment = "positive"
    return sentiment


#Add sentiment labels to to DataFrame based on api response description
def add_sentiment_label(df, tokenizer, model):
    try:
        df['sentiment'] = df['description'].apply(lambda x: perform_sentiment_analysis(x, tokenizer, model))
        return df
    except Exception as e:
        logging.error(f'Sentiment analysis failed: {e}')
        return None

    
#insert dataframes to sql database
def insert_data_to_database(df, table_name, connection):
    try:
        if connection:
            df.to_sql(table_name, connection, if_exists = 'append', index = False)
            logging.info(f'{df} appended to news_analysis_db successfully')
    except Exception as e:
        logging.error(f'{table_name} failed to append to news_analysis_db: {e}')


def add_category_and_append_data(df, category_name, connection):
    category_id_mapping = define_news_categories()
    category_id = category_id_mapping[category_name]
    try:
        df.insert(0, 'categoryID', category_id)
        df = add_count_sentiment(df)
        insert_data_to_database(df, category_name, connection)
    except Exception as e:
        logging.error(f'Failed to add categoryid and append data to {category_name} table: {e}')

#crawl news data, perform sentitnet analyssis, insert datatframe to dataabse
def crawl_and_process_categories(category_name):
    try:

        connection = get_database_connection()

        # Fetch news data
        response = get_api(category_name)


        # Instantiate tokenizer and model
        tokenizer, model = instantiate_model()
        
        #Create dataframe
        df = create_dataframe(response)
        if df is not None:
            df = add_sentiment_label(df, tokenizer, model)
            if df is not None:
                add_category_and_append_data(df.copy(), category_name, connection)

    except Exception as e:
        logging.error(f"Error crawling and processing category {category_name}: {e}")
    
    finally:
        connection.close()
        logging.info("Database connection closed for daily analysis.")


def main(category):
    crawl_and_process_categories(category)


if __name__ == "__main__":
    main()

