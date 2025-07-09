import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import logging
import time
import os 

logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a")

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con=engine, schema='public', if_exists='replace', index=False)
    print(f"✅ Table '{table_name}' uploaded successfully to schema 'public'.")

def load_raw_data():
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            df = pd.read_csv(file_path)  
            logging.info(f"📦 Ingesting '{file}' with shape {df.shape}")
            table_name = file.split('.')[0].lower()
            ingest_db(df, table_name, engine)
    end = time.time()  
    total_time = (end - start) / 60
    logging.info('--------------ingestion complete----------------')
    logging.info(f'Total time taken : {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()
