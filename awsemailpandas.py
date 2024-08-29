# awsmailpandas.py

import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd

# Loading variables from .env file
load_dotenv('sample.env')

def fetch_and_save_data():
    connection = None
    cursor = None
    file_path = "C:/Users/DELL/pythonreadvalues.csv"  # Specify the path where you want to save the CSV file
    
    try:
        connection = psycopg2.connect(user=os.getenv("user"),
                                      password=os.getenv("password"),
                                      host=os.getenv("host"),
                                      port=os.getenv("port"),
                                      database=os.getenv("database"))
        cursor = connection.cursor()
        
        query = '''SELECT * FROM finaltable 
                   WHERE (is_no_show='FALSE' AND is_rescheduled='FALSE')
                   AND (is_rescheduled='FALSE' AND is_cancelled='TRUE');'''
        cursor.execute(query)
        records = cursor.fetchall()
        
        # Save the fetched data into a DataFrame and then to a CSV file
        df = pd.DataFrame(records)
        df.to_csv(file_path, index=False)
        
        return file_path  # Return the file path to be used in awsfile
    except Exception as error:
        print(error)
    finally:
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
