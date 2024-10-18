import os
import sys
import pandas as pd
from typing import List
from sklearn.model_selection import train_test_split
from astrapy import DataAPIClient

from src.exception.exception import Credit_Risk_Exception
from src.logging.logger import logging
from src.entity.config_entity import DataIngestionConfig
from src.entity.artifact_entity import DataIngestionArtifact

from dotenv import load_dotenv
load_dotenv()

class DataIngestion:

    def __init__(self, data_ingestion_config: DataIngestionConfig):

        try:
            self.data_ingestion_config = data_ingestion_config

            # Initialize Astra DB client
            self.client = DataAPIClient(os.getenv("ASTRA_DB_TOKEN"))
            self.db = self.client.get_database(
                os.getenv("ASTRA_DB_ENDPOINT"), 
                keyspace=os.getenv("ASTRA_DB_KEYSPACE")
            )
            self.collection = self.db.get_collection(self.data_ingestion_config.collection_name)

            logging.info("Successfully initialized Astra DB client and connected to collection.")

        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def export_collection_dataframe(self):

        try:

            data = self.collection.find({}) 
            df = pd.DataFrame(data)

            if "_id" in df.columns:
                df = df.drop(columns=["_id"], axis=1)
                df.replace({"na": pd.NA}, inplace=True)

            logging.info("Exported data from collection into DataFrame.")
            return df

        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def export_data_into_feature_store(self, dataframe: pd.DataFrame):
        try:
            feature_store_filepath = self.data_ingestion_config.feature_store_file_path
            dir_path = os.path.dirname(feature_store_filepath)
            os.makedirs(dir_path, exist_ok=True)

            dataframe.to_csv(feature_store_filepath, index=False, header=True)
            logging.info("Exported data into feature store at: " + feature_store_filepath)
            return dataframe

        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def split_data(self, dataframe: pd.DataFrame):
        try:
            train_set, test_set = train_test_split(
                dataframe, 
                test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logging.info("Performed Train Test Split.")

            train_dir = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(train_dir, exist_ok=True)

            train_set.to_csv(self.data_ingestion_config.training_file_path, index=False, header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path, index=False, header=True)

            logging.info("Exported train and test datasets.")

        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def initiate_data_ingestion(self):
        
        try:
            dataframe = self.export_collection_dataframe()
            dataframe = self.export_data_into_feature_store(dataframe)
            self.split_data(dataframe)

            data_ingestion_artifact = DataIngestionArtifact(
                trained_file_path=self.data_ingestion_config.training_file_path,
                test_file_path=self.data_ingestion_config.testing_file_path
            )

            return data_ingestion_artifact

        except Exception as e:
            raise Credit_Risk_Exception(e, sys)
