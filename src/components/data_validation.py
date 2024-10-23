import os,sys
import pandas as pd
from src.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from src.entity.config_entity import DataValidationConfig
from src.logging.logger import logging
from src.exception.exception import Credit_Risk_Exception
from src.constants.training_pipeline import SCHEMA_FILE_PATH
from scipy.stats import ks_2samp
from src.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        """Read data from a CSV file."""
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        """Validate if the number of columns matches the schema."""
        try:
            required_columns = len(self.schema_config['columns'])
            actual_columns = len(dataframe.columns)
            logging.info(f"Required number of columns: {required_columns}, Actual number of columns: {actual_columns}")
            return required_columns == actual_columns
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def validate_column_data_types(self, dataframe: pd.DataFrame) -> bool:
        """Validate that each column in the dataframe has the correct data type."""
        try:
            for column, expected_type in self.schema_config['columns'].items():
                if not pd.api.types.is_dtype_equal(dataframe[column].dtype, expected_type):
                    logging.error(f"Column {column} has incorrect data type. Expected: {expected_type}, Got: {dataframe[column].dtype}")
                    return False
            return True
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def validate_no_missing_values(self, dataframe: pd.DataFrame) -> bool:
        """Validate that the dataframe contains no missing/null values."""
        try:
            missing_values = dataframe.isnull().sum().sum()
            if missing_values > 0:
                logging.error(f"Dataframe contains {missing_values} missing values.")
                return False
            return True
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def validate_class_balance(self, dataframe: pd.DataFrame, target_column: str) -> bool:
        """Validate that the target column has a reasonable balance of classes."""
        try:
            class_distribution = dataframe[target_column].value_counts(normalize=True)
            imbalance_threshold = self.schema_config.get('imbalance_threshold', 0.1)
            if any(class_distribution < imbalance_threshold):
                logging.warning(f"Class imbalance detected in column {target_column}. Distribution: {class_distribution}")
            return True
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def detect_dataset_drift(self, base_df: pd.DataFrame, current_df: pd.DataFrame, threshold: float = 0.05) -> bool:
        """Detect dataset drift using KS test."""
        try:
            status = True
            report = {}

            for column in base_df.columns:
                base_col = base_df[column]
                current_col = current_df[column]
                ks_test_result = ks_2samp(base_col, current_col)
                drift_detected = ks_test_result.pvalue < threshold
                report[column] = {"p_value": float(ks_test_result.pvalue), "drift_status": drift_detected}

                if drift_detected:
                    status = False

            self.save_drift_report(report)
            return status
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def save_drift_report(self, report: dict):
        """Save drift report to a YAML file."""
        try:
            drift_report_filepath = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(drift_report_filepath), exist_ok=True)
            write_yaml_file(file_path=drift_report_filepath, content=report)
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def validate_and_save(self, dataframe: pd.DataFrame, valid_file_path: str):
        """Save the validated dataframe to CSV."""
        try:
            dir_path = os.path.dirname(valid_file_path)
            os.makedirs(dir_path, exist_ok=True)
            dataframe.to_csv(valid_file_path, index=False, header=True)
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

    def initiate_data_validation(self) -> DataValidationArtifact:
        """Initiate the data validation process."""
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            train_df = self.read_data(train_file_path)
            test_df = self.read_data(test_file_path)

            # Apply all validations
            if not self.validate_number_of_columns(train_df):
                raise ValueError("Train dataframe does not have the required columns")
            if not self.validate_number_of_columns(test_df):
                raise ValueError("Test dataframe does not have the required columns")

            if not self.validate_column_data_types(train_df):
                raise ValueError("Train dataframe has incorrect data types")
            if not self.validate_column_data_types(test_df):
                raise ValueError("Test dataframe has incorrect data types")

            if not self.validate_no_missing_values(train_df):
                raise ValueError("Train dataframe contains missing values")
            if not self.validate_no_missing_values(test_df):
                raise ValueError("Test dataframe contains missing values")

            # Drift detection
            drift_status = self.detect_dataset_drift(base_df=train_df, current_df=test_df)

            # Validate class balance
            self.validate_class_balance(train_df, target_column='credit_risk')

            # Save validated datasets
            self.validate_and_save(train_df, self.data_validation_config.valid_train_file_path)
            self.validate_and_save(test_df, self.data_validation_config.valid_test_file_path)

            return DataValidationArtifact(
                validation_status=drift_status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )
        except Exception as e:
            raise Credit_Risk_Exception(e, sys)

