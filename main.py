import sys
from src.components.data_ingestion import DataIngestion
from src.components.data_validation import DataValidation
from src.exception.exception import Credit_Risk_Exception
from src.logging.logger import logging
from src.entity.config_entity import DataIngestionConfig,DataValidationConfig
from src.entity.config_entity import TrainingPipelineConfig

if __name__=="__main__":
    
    try:
        trainingpipelineconfig=TrainingPipelineConfig()
        dataingestionconfig=DataIngestionConfig(trainingpipelineconfig)
        data_ingestion=DataIngestion(dataingestionconfig)
        logging.info("Initiate the data ingestion")
        dataingestionartifact=data_ingestion.initiate_data_ingestion()
        logging.info("Data Ingestion Completed")
        print(dataingestionartifact)

        datavalidationconfig=DataValidationConfig(trainingpipelineconfig)
        data_validation=DataValidation(dataingestionartifact,datavalidationconfig)
        logging.info("Initiate the data validation")
        datavalidationartifact=data_validation.initiate_data_validation()
        logging.info("Data Validation Completed")
        print(datavalidationartifact)

    except Exception as e:
        raise Credit_Risk_Exception(e,sys)