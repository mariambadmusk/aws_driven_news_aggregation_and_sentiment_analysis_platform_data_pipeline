#import necessary dependencies
from bs4 import BeautifulSoup
import requests 
import pandas as pd
from collections import Counter
import configparser
import logging
from logging_config import setup_logging
from utils import define_news_categories, get_database_connection

# set up logging
setup_logging()

def read_sql_from_db(connection, table_name): 
    try:
        result = pd.read_sql_query(f"""
            SELECT *  
            FROM {table_name}
            WHERE pubDate BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE
        """, connection)
        return result
    except Exception as e:
        logging.error(f"Failed to read data from the database: {e}")
        return None
    


#create weekly fact table with averageCountAndLength, categoryID, wordCount, articleLentgh and popular sentiments columns
def populate_weekly_fact_table(result, categoryID):
    averageCountAndLength = result.groupby([pd.Grouper(key = 'PubDate', freq = 'W'), 'categoryID']).agg({
        'wordCount': 'mean',
        'articleLength': 'mean'
    }).reset_index()
    
    sentiment_counts = Counter(result['Sentiment'])
    maxCounts = max(sentiment_counts, key = sentiment_counts.get)

    weekly_data = pd.DataFrame ({
        'categoryID':categoryID, 
        'wordCount': averageCountAndLength['wordCount'], 
        'articleLength': averageCountAndLength['articleLength'],
        'popularSentiment': maxCounts
        })
    return weekly_data


#insert dataframes to sql database
def insert_data_to_database(df, news_weekly_fact_table, connection):
    try:
        if connection:
            df.to_sql(news_weekly_fact_table, connection, if_exists = 'append', index = False)
            logging.info('weekly_data appended to weekly_fact_table sucessfully')
        else:
            logging.error('Connection to database failed: cannot append weekly_table')
    except Exception as e:
        logging.error(f'weekly_table failed to append to news_analysis_db: {e}')

#apply to all catgrories:
def main():
    connection = get_database_connection()
    if not connection:
        logging.error("Failed to establish database connection.")
        return None
    
    try:
        table_name = "news_weekly_fact_table"
        category_id_mapping = define_news_categories()
        
        for category, categoryID in category_id_mapping.items():
            result = read_sql_from_db(connection, category)
            if result is not None:
                weekly_data = populate_weekly_fact_table(result, categoryID)
                insert_data_to_database(weekly_data, table_name, connection)
            else:
                logging.error(f"Failed to process data for {category}")
    finally:
        connection.close()
        logging.info("Database connection closed for weekly analysis.")


if __name__ == "__main__":
    main()


