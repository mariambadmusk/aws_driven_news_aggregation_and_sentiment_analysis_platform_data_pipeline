#import necessary dependencies
import pandas as pd
import configparser
import psycopg2

# Read configuration settings from the .env file
config = configparser.ConfigParser()
config.read('.env')

#Retrieve the PostgreSQL database password from the configuration file
db_password = config['DB']['password']

#set database parameters
db_params = {
    'host': 'localhost',
    'user': 'postgres',
    'password' : db_password
        }


#category, date, averageArticleCount, averageArticleLength
categoryDict = {"business":101, "crime": 102 ,"education": 103, "entertainment":104, "health":105, "science":106}
categoryList = pd.DataFrame(categoryDict, columns = ["categoryID","category"])

#connect to news_sentiment_analysis_database
db_params = {
    'dbname': 'news_sentiment_analysis_database',
    'host': 'localhost',
    'user': 'postgres',
    'password' : db_password
        }

try:
    #connect to database
    connection = psycopg2.connect(**db_params)
    connection.autocommit = True
    cursor = connection.cursor()

    # create news_sentiment_analysis_database
    cursor.execute("CREATE DATABASE news_sentiment_analysis_database;")

    # Connect to news_sentiment_analysis_database
    db_params['dbname'] = 'news_sentiment_analysis_database'
    connection = psycopg2.connect(**db_params)
    connection.autocommit = True
    cursor = connection.cursor()

    #create dimension tables
    categories = ["business", "crime", "education", "entertainment", "health", "science"]
    for category in categories:
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {category} (
                            categoryID INT PRIMARY KEY,
                            {category}ArticleID VARCHAR(40),
                            {category}Title VARCHAR(300),
                            {category}Description VARCHAR(1000),
                            {category}Links VARCHAR(500),
                            {category}PubDate DATE,
                            {category}Content VARCHAR(3500),
                            {category}WordCount INT,
                            {category}ArticleLength INT,
                            {category}Sentiment VARCHAR(10)
                        );
                        """)

    #create weekly facts and category tables
    cursor.execute ("""CREATE TABLE IF NOT EXISTS news_weekly_fact_table (
                        categoryID INT PRIMARY KEY,
                        wordCount INT,
                        articleLength INT,
                        popularSentiment VARCHAR(10),
                        startDate DATE,
                        endDate DATE
                        );""")

    cursor.execute ("""CREATE TABLE IF NOT EXISTS category (
                        categoryID INT PRIMARY KEY,
                        category VARCHAR(30)
                        );""")

    print('All databases and tables created')

except psycopg2.Error as e:
    print("Error:", e)

finally:
    if connection:
        connection.close()
