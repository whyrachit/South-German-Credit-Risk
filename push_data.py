import os
import sys
import json
import pandas as pd
from dotenv import load_dotenv
from astrapy import DataAPIClient
from src.exception.exception import Credit_Risk_Exception
from src.logging.logger import logging

load_dotenv()
ASTRA_DB_TOKEN = os.getenv("ASTRA_DB_TOKEN")
ASTRA_DB_ENDPOINT = os.getenv("ASTRA_DB_ENDPOINT")
ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")
ASTRA_DB_COLLECTION = os.getenv("ASTRA_DB_COLLECTION")

class CreditDataExtract:
    def __init__(self):
        try:
            # Initialize the Astra DB client
            self.client = DataAPIClient(ASTRA_DB_TOKEN)
            self.db = self.client.get_database(ASTRA_DB_ENDPOINT, keyspace=ASTRA_DB_KEYSPACE)
            logging.info("Successfully connected to Astra DB.")
        except Exception as e:
            logging.error("Error connecting to Astra DB.")
            raise Credit_Risk_Exception(e, sys)

    def csv_to_json_convertor(self, file_path):

        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)

            if 'id' not in data.columns:
                data.insert(0, 'id', range(1, 1 + len(data)))

            records = list(json.loads(data.T.to_json()).values())
            logging.info(f"Converted {len(records)} records to JSON.")
            return records
        
        except Exception as e:
            logging.error("Error converting CSV to JSON.")
            raise Credit_Risk_Exception(e, sys)

    def insert_data_astra(self, records):

        try:
            collection = self.db.get_collection(ASTRA_DB_COLLECTION)
            for record in records:
                collection.insert_one(record)

            logging.info("Collection Added to Astra DB")
        
        except Exception as e:
            logging.error("Error inserting records into Astra DB.")
            raise Credit_Risk_Exception(e, sys)


if __name__ == '__main__':

    try:
        FILE_PATH = "credit_data/SouthGermanCreditData.csv"
        network_obj = CreditDataExtract()
        records = network_obj.csv_to_json_convertor(FILE_PATH)
        num_records = network_obj.insert_data_astra(records)
        print(f"Successfully inserted {num_records} records into Astra DB.")

    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")
        raise