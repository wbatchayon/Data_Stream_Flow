"""
Data Streaming Module - Spark Streaming CSV Reader
==================================================
Streams CSV data using Spark Streaming for real-time processing.

Author: William BATCHAYON
Version: 1.0.0
"""

import logging
import os
import sys
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class StreamingError(Exception):
    """Custom exception for streaming errors."""

    pass


def get_csv_schema() -> StructType:
    """
    Returns the schema for CSV data.
    This should be customized based on actual data structure.

    Returns:
        StructType schema for CSV data
    """
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )


def spark_streaming_1(
    input_path: str = "/tmp/spark_processed.csv",
    app_name: str = "SparkStreaming1",
    processing_time: int = 30,
) -> bool:
    """
    Reads CSV data as a Spark Streaming source for real-time processing.

    Args:
        input_path: Path to the CSV file or directory
        app_name: Name of the Spark application
        processing_time: How long to run the streaming (seconds)

    Returns:
        True if successful

    Raises:
        StreamingError: If streaming fails
    """
    logger.info(f"Starting Spark Streaming 1: {input_path}")

    spark = None
    try:
        # Initialize Spark session with streaming support
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Get the schema
        schema = get_csv_schema()

        # Check if input path exists
        if not os.path.exists(input_path):
            # Try to find the CSV in subdirectory
            csv_dir = os.path.dirname(input_path)
            if os.path.exists(csv_dir):
                for f in os.listdir(csv_dir):
                    if f.endswith(".csv"):
                        input_path = os.path.join(csv_dir, f)
                        break

        if not os.path.exists(input_path):
            raise StreamingError(f"Input path does not exist: {input_path}")

        # Read as static DataFrame first (for batch processing simulation)
        logger.info(f"Reading CSV from: {input_path}")

        # For file-based streaming simulation
        df = spark.read.schema(schema).option("header", "true").csv(input_path)

        # Log schema
        logger.info(f"Schema: {df.schema.simpleString()}")

        # Process data - example transformations
        processed_df = df.withColumn("processed_timestamp", F.current_timestamp()).withColumn(
            "record_hash", F.hash(F.concat(*df.columns))
        )

        # Show sample data
        logger.info("Sample processed data:")
        processed_df.show(5, truncate=False)

        # Count records
        count = processed_df.count()
        logger.info(f"Processed {count:,} records")

        logger.info(f"Spark Streaming 1 completed successfully")
        return True

    except Exception as e:
        raise StreamingError(f"Spark Streaming 1 failed: {e}") from e

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    try:
        result = spark_streaming_1()
        logger.info("Spark Streaming 1 finished successfully")
        sys.exit(0)
    except StreamingError as e:
        logger.error(f"Streaming failed: {e}")
        sys.exit(1)
