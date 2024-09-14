from utils import get_database_connection
import logging
from logging_config import setup_logging

# Set up logging
setup_logging()

def create_tables(connection, cursor):
    try:  
        create_business_table = """
        CREATE TABLE IF NOT EXISTS business (
            categoryID SERIAL PRIMARY KEY,
            businessArticleID VARCHAR(40),
            businessTitle VARCHAR(300),
            businessDescription VARCHAR(1000),
            businessLinks VARCHAR(500),
            businessPubDate DATE,
            businessContent VARCHAR(3500),
            businessWordCount INT,
            businessArticleLength INT,
            businessSentiment VARCHAR(10)
        );
        """
        
        create_crime_table = """
        CREATE TABLE IF NOT EXISTS crime (
            categoryID SERIAL PRIMARY KEY,
            crimeArticleID VARCHAR(40),
            crimeTitle VARCHAR(300),
            crimeDescription VARCHAR(1000),
            crimeLinks VARCHAR(500),
            crimePubDate DATE,
            crimeContent VARCHAR(3500),
            crimeWordCount INT,
            crimeArticleLength INT,
            crimeSentiment VARCHAR(10)
        );
        """
        
        create_education_table = """
        CREATE TABLE IF NOT EXISTS education (
            categoryID SERIAL PRIMARY KEY,
            educationArticleID VARCHAR(40),
            educationTitle VARCHAR(300),
            educationDescription VARCHAR(1000),
            educationLinks VARCHAR(500),
            educationPubDate DATE,
            educationContent VARCHAR(3500),
            educationWordCount INT,
            educationArticleLength INT,
            educationSentiment VARCHAR(10)
        );
        """
        
        create_entertainment_table = """
        CREATE TABLE IF NOT EXISTS entertainment (
            categoryID SERIAL PRIMARY KEY,
            entertainmentArticleID VARCHAR(40),
            entertainmentTitle VARCHAR(300),
            entertainmentDescription VARCHAR(1000),
            entertainmentLinks VARCHAR(500),
            entertainmentPubDate DATE,
            entertainmentContent VARCHAR(3500),
            entertainmentWordCount INT,
            entertainmentArticleLength INT,
            entertainmentSentiment VARCHAR(10)
        );
        """
        
        create_health_table = """
        CREATE TABLE IF NOT EXISTS health (
            categoryID SERIAL PRIMARY KEY,
            healthArticleID VARCHAR(40),
            healthTitle VARCHAR(300),
            healthDescription VARCHAR(1000),
            healthLinks VARCHAR(500),
            healthPubDate DATE,
            healthContent VARCHAR(3500),
            healthWordCount INT,
            healthArticleLength INT,
            healthSentiment VARCHAR(10)
        );
        """
        
        create_science_table = """
        CREATE TABLE IF NOT EXISTS science (
            categoryID SERIAL PRIMARY KEY,
            scienceArticleID VARCHAR(40),
            scienceTitle VARCHAR(300),
            scienceDescription VARCHAR(1000),
            scienceLinks VARCHAR(500),
            sciencePubDate DATE,
            scienceContent VARCHAR(3500),
            scienceWordCount INT,
            scienceArticleLength INT,
            scienceSentiment VARCHAR(10)
        );
        """
        
        create_weekly_fact_table = """
        CREATE TABLE IF NOT EXISTS weekly_fact (
            categoryID INT,
            wordCount FLOAT,
            articleLength FLOAT,
            popularSentiment VARCHAR(10),
            PRIMARY KEY (categoryID)
        );
        """

        create_category_table = """
        CREATE TABLE IF NOT EXISTS category (
            categoryID SERIAL PRIMARY KEY,
            category_name VARCHAR(40)
        );
        """
        
        # Execute SQL commands
        cursor.execute(create_business_table)
        cursor.execute(create_crime_table)
        cursor.execute(create_education_table)
        cursor.execute(create_entertainment_table)
        cursor.execute(create_health_table)
        cursor.execute(create_science_table)
        cursor.execute(create_weekly_fact_table)
        cursor.execute(create_category_table)
    
        logging.info("Tables created successfully.")
        
    except Exception as e:
        logging.error(f"Failed to create tables: {e}")
        connection.rollback()

def insert_category_data(connection, cursor):
    try:
        insert_category = """
        INSERT INTO category (category_name) VALUES
        ('business'),
        ('crime'),
        ('education'),
        ('entertainment'),
        ('health'),
        ('science');
        """
        
        cursor.execute(insert_category)
        logging.info("Category data inserted successfully.")
        
    except Exception as e:
        logging.error(f"Failed to insert category data: {e}")
        connection.rollback()

def main():
    try:
        connection = get_database_connection()
        cursor = connection.cursor()
        connection.autocommit = True
        create_tables(connection, cursor)
        insert_category_data(connection, cursor)
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
