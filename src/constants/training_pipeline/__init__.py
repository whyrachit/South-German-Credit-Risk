import os

TARGET_COLUMN= "credit_risk"
PIPELINE_NAME : str = "Network Security"
ARTIFACT_DIR: str = "Artifacts"
FILE_NAME: str = "SouthGermanCreditData.csv"

TRAIN_FILE_NAME: str = "train.csv"
TEST_FILE_NAME: str = "test.csv"

SCHEMA_FILE_PATH = os.path.join("data_schema","schema.yaml")
SAVED_MODEL_DIR= os.path.join("saved_models")
MODEL_FILE_NAME ="model.pkl"

DATA_INGESTION_COLLECTION_NAME: str = "german_credit_risk"
DATA_INGESTION_DATABASE_NAME: str = "Bank_Credit_Risk"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR: str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO: float = .2

DATA_VALIDATION_DIR_NAME: str = "data_validation"
DATA_VALIDATION_VALID_DIR: str = "validated"
DATA_VALIDATION_INVALID_DIR: str = "invalid"
DATA_VALIDATION_DRIFT_REPORT_DIR: str = "drift_report"
DATA_VALIDATION_DRIFT_REPORT_FILE_NAME: str = "report.yaml"