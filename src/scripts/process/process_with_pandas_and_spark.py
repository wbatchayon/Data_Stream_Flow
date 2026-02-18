"""
Data Processing Module - Pandas & Spark Processing
==================================================
Processes data using Pandas for cleaning and Spark for distributed processing.

Author: William BATCHAYON
Version: 1.0.0
"""

import logging
import os
import sys
from typing import Optional, Dict, Any

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataProcessingError(Exception):
    """Custom exception for data processing errors."""
    pass


def process_with_pandas(
    input_path: str = "/tmp/remote_data.csv",
    output_path: str = "/tmp/pandas_processed.csv"
) -> str:
    """
    Processes CSV data using Pandas for data cleaning and transformation.
    
    Args:
        input_path: Path to the input CSV file
        output_path: Path to save the processed CSV
        
    Returns:
        Path to the processed file
        
    Raises:
        DataProcessingError: If processing fails
    """
    logger.info(f"Starting Pandas processing: {input_path}")
    
    try:
        # Load the CSV file
        df = pd.read_csv(input_path)
        original_rows = len(df)
        logger.info(f"Loaded {original_rows:,} rows from {input_path}")
        
        # Data cleaning operations
        # Remove duplicate rows
        df = df.drop_duplicates()
        duplicates_removed = original_rows - len(df)
        logger.info(f"Removed {duplicates_removed} duplicate rows")
        
        # Handle missing values - drop rows with all NaN
        df = df.dropna(how='all')
        
        # Fill numeric columns with median
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if df[col].isna().sum() > 0:
                median_value = df[col].median()
                df[col].fillna(median_value, inplace=True)
                logger.info(f"Filled {col} missing values with median: {median_value}")
        
        # Handle string columns - fill with empty string
        string_cols = df.select_dtypes(include=['object']).columns
        for col in string_cols:
            if df[col].isna().sum() > 0:
                df[col].fillna('', inplace=True)
        
        # Data validation
        if len(df) == 0:
            raise DataProcessingError("No data remaining after processing")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save processed data
        df.to_csv(output_path, index=False)
        
        final_rows = len(df)
        logger.info(f"Pandas processing completed: {final_rows:,} rows saved to {output_path}")
        
        return output_path
        
    except FileNotFoundError as e:
        raise DataProcessingError(f"Input file not found: {input_path}") from e
    except pd.errors.EmptyDataError as e:
        raise DataProcessingError("Input file is empty") from e
    except Exception as e:
        raise DataProcessingError(f"Pandas processing failed: {e}") from e


def process_with_spark(
    input_path: str = "/tmp/pandas_processed.csv",
    output_path: str = "/tmp/spark_processed.csv",
    app_name: str = "SparkProcessing"
) -> str:
    """
    Processes data using Apache Spark for distributed computation.
    
    Args:
        input_path: Path to the input CSV file
        output_path: Path to save the processed CSV
        app_name: Name of the Spark application
        
    Returns:
        Path to the processed file
        
    Raises:
        DataProcessingError: If processing fails
    """
    logger.info(f"Starting Spark processing: {input_path}")
    
    spark = None
    try:
        # Initialize Spark session with optimized configuration
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.shuffle.service.enabled", "true") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        # Load the CSV file with schema inference
        logger.info("Loading data into Spark DataFrame...")
        df = spark.read.csv(
            input_path,
            header=True,
            inferSchema=True
        )
        
        # Log schema
        logger.info(f"Schema: {df.schema.simpleString()}")
        
        # Get row count
        count = df.count()
        logger.info(f"Loaded {count:,} rows")
        
        # Process data - example: filter and aggregate
        # Note: This should be customized based on actual data schema
        processed_df = df
        
        # Save the processed data
        os.makedirs(output_path, exist_ok=True)
        
        # Write as CSV (single file output for simplicity)
        processed_df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        
        logger.info(f"Spark processing completed: saved to {output_path}")
        
        # Return path to the output file
        return output_path
        
    except Exception as e:
        raise DataProcessingError(f"Spark processing failed: {e}") from e
        
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


def process_with_pandas_and_spark(
    input_csv: str = "/tmp/remote_data.csv",
    pandas_output: str = "/tmp/pandas_processed.csv",
    spark_output: str = "/tmp/spark_processed.csv"
) -> Dict[str, str]:
    """
    Orchestrates the complete data processing pipeline using Pandas and Spark.
    
    Args:
        input_csv: Path to the input CSV file
        pandas_output: Path for Pandas processed output
        spark_output: Path for Spark processed output
        
    Returns:
        Dictionary with output paths
    """
    logger.info("Starting complete data processing pipeline")
    
    # Step 1: Pandas processing
    pandas_result = process_with_pandas(
        input_path=input_csv,
        output_path=pandas_output
    )
    
    # Step 2: Spark processing
    spark_result = process_with_spark(
        input_path=pandas_result,
        output_path=spark_output
    )
    
    logger.info("Complete data processing pipeline finished")
    
    return {
        "pandas_output": pandas_result,
        "spark_output": spark_result
    }


if __name__ == "__main__":
    try:
        result = process_with_pandas_and_spark()
        logger.info(f"Processing complete: {result}")
        sys.exit(0)
    except DataProcessingError as e:
        logger.error(f"Processing failed: {e}")
        sys.exit(1)
