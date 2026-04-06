import great_expectations as gx
import pandas as pd
import logging
from airflow.exceptions import AirflowFailException
import sys
import os

# Ensure the parent directory is in path to import constants
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.constants import OUTPUT_PATH

def validate_bronze_parquet(file_name: str, **kwargs):
    """
    Validates the newly extracted bronze data using Great Expectations.
    Raises an AirflowFailException if the data quality checks fail, 
    preventing bad data from reaching the Silver/Gold tables.
    """
    logging.info(f"Starting Great Expectations validation for {file_name}...")
    
    # We are currently loading the CSV for ease, 
    # if you switch to Parquet in the extractor, use pd.read_parquet instead
    file_path = f"{OUTPUT_PATH}/{file_name}.csv"
    
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        logging.error(f"Cannot find file {file_path} to validate.")
        raise AirflowFailException(f"Validation failed: Data file {file_path} not found.")

    if df.empty:
        logging.warning("Dataframe is empty, nothing to validate.")
        return

    # Great Expectations API changed across versions.
    # Use GE where available, and a strict pandas fallback when from_pandas is absent.
    try:
        dataset = gx.from_pandas(df)

        logging.info("Running Expectation: ID should not be null...")
        res1 = dataset.expect_column_values_to_not_be_null(column="id")

        logging.info("Running Expectation: Title should not be null...")
        res2 = dataset.expect_column_values_to_not_be_null(column="title")

        logging.info("Running Expectation: Score should be >= 0...")
        res3 = dataset.expect_column_values_to_be_between(column="score", min_value=0)

        is_successful = res1.success and res2.success and res3.success
    except AttributeError:
        logging.warning("great_expectations.from_pandas not available; using strict pandas fallback checks.")
        res1 = type("Result", (), {"success": df["id"].notna().all()})
        res2 = type("Result", (), {"success": df["title"].notna().all()})
        res3 = type("Result", (), {"success": (df["score"].fillna(-1) >= 0).all()})
        is_successful = res1.success and res2.success and res3.success
    
    if not is_successful:
        logging.error("Great Expectations Validation FAILED:")
        if not res1.success: logging.error("-> Failed on column 'id' being not null")
        if not res2.success: logging.error("-> Failed on column 'title' being not null")
        if not res3.success: logging.error("-> Failed on column 'score' being >= 0")
        
        # Stop Airflow from progressing to the DBT step
        raise AirflowFailException("Data Quality Check Failed. See Airflow logs for details.")
        
    logging.info("✅ All Data Quality checks PASSED! Bronze data is validated.")
