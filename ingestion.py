import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create SQLite engine
engine = create_engine('sqlite:///inventory.db')

# Function to ingest DataFrame into SQLite database
def ingest_db(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully ingested: {table_name}")
    except Exception as e:
        logging.error(f"Failed to ingest {table_name}: {str(e)}")

# Function to load all CSV files from data/ folder
def load_data():
    start = time.time()
    for file in os.listdir('data'):
        if file.endswith('.csv'):
            file_path = os.path.join('data', file)
            try:
                df = pd.read_csv(file_path)
                logging.info(f"Ingesting {file} into DB")
                ingest_db(df, file[:-4], engine)
            except Exception as e:
                logging.error(f"Error processing {file}: {str(e)}")

    end = time.time()
    total_time = (end - start) / 60
    logging.info('---------- Ingestion Complete ----------')
    logging.info(f'Total time taken: {total_time:.2f} minutes')

# Run the script
if __name__ == '__main__':
    load_data()
