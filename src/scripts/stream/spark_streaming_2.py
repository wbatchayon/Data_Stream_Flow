"""
Spark Streaming Module - Kafka Consumer
======================================
Consumes data from Kafka using Spark Streaming.

Author: William BATCHAYON
Version: 1.0.0
"""

import logging
import os
import sys
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class StreamingError(Exception):
    """Custom exception for streaming errors."""

    pass


def spark_streaming_2(
    kafka_servers: str = "kafka:9092",
    topic: str = "data_topic",
    output_path: str = "/tmp/kafka_output",
    checkpoint_path: str = "/tmp/checkpoints",
    app_name: str = "SparkStreaming2",
    processing_time: int = 30,
) -> bool:
    """
    Consumes data from Kafka using Spark Streaming.

    Args:
        kafka_servers: Kafka bootstrap servers
        topic: Kafka topic to subscribe to
        output_path: Path to save the streaming output
        checkpoint_path: Path for checkpointing
        app_name: Name of the Spark application
        processing_time: How long to run the streaming (seconds)

    Returns:
        True if successful

    Raises:
        StreamingError: If streaming fails
    """
    logger.info(f"Starting Spark Streaming 2: consuming from {topic}")

    spark = None
    try:
        # Initialize Spark session with streaming and Kafka support
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.streaming.checkpointLocation", checkpoint_path)
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config(
                "spark.kafka.spark_streaming_2.pkgs",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Read from Kafka
        logger.info(f"Connecting to Kafka at {kafka_servers}")

        df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", 1000)
            .load()
        )

        logger.info(f"Subscribed to Kafka topic: {topic}")

        # Parse the JSON value
        parsed_df = df.select(
            F.from_json(F.col("value").cast("string"), "id STRING, data STRING").alias("parsed"),
            F.col("timestamp"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
        ).select("parsed.*", "timestamp", "topic", "partition", "offset")

        # Add processing timestamp
        processed_df = parsed_df.withColumn("processed_at", F.current_timestamp())

        # Log schema
        logger.info(f"Schema: {processed_df.schema.simpleString()}")

        # Write to console (for debugging) and file
        query = (
            processed_df.writeStream.format("json")
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .trigger(processingTime="10 seconds")
            .start()
        )

        logger.info(f"Streaming started, writing to {output_path}")

        # Wait for processing time
        query.awaitTermination(processing_time)

        # Stop the query gracefully
        query.stop()

        # Count processed records
        logger.info("Spark Streaming 2 completed successfully")
        return True

    except Exception as e:
        raise StreamingError(f"Spark Streaming 2 failed: {e}") from e

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    try:
        result = spark_streaming_2()
        logger.info("Spark Streaming 2 finished successfully")
        sys.exit(0)
    except StreamingError as e:
        logger.error(f"Streaming failed: {e}")
        sys.exit(1)
