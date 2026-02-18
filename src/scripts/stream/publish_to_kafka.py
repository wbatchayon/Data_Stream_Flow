"""
Kafka Producer Module - Publish to Kafka Topic
==============================================
Publishes data to Kafka topics for streaming processing.

Author: William BATCHAYON
Version: 1.0.0
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class KafkaPublishError(Exception):
    """Custom exception for Kafka publishing errors."""

    pass


def publish_to_kafka(
    input_path: str = "/tmp/generated_data.json",
    bootstrap_servers: str = "kafka:9092",
    topic: str = "data_topic",
    max_retries: int = 3,
) -> int:
    """
    Publishes generated data to a Kafka topic.

    Args:
        input_path: Path to the JSON data file
        bootstrap_servers: Kafka bootstrap servers
        topic: Kafka topic name
        max_retries: Maximum number of connection retries

    Returns:
        Number of records published

    Raises:
        KafkaPublishError: If publishing fails
    """
    logger.info(f"Starting Kafka publish to topic: {topic}")

    producer = None
    try:
        # Initialize Kafka producer with optimized settings
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type="gzip",  # Compress messages
            linger_ms=10,  # Batch messages
            batch_size=16384,
            buffer_memory=33554432,
            max_block_ms=30000,
            request_timeout_ms=30000,
        )

        logger.info(f"Connected to Kafka at {bootstrap_servers}")

        # Verify topic exists or will be auto-created
        logger.info(f"Target topic: {topic}")

        # Load the generated data
        with open(input_path, "r") as f:
            data = json.load(f)

        if not isinstance(data, list):
            data = [data]

        logger.info(f"Publishing {len(data):,} records to Kafka")

        # Publish each record
        published_count = 0
        failed_count = 0

        for record in data:
            try:
                # Add metadata to the record
                enriched_record = {
                    **record,
                    "_metadata": {
                        "published_at": datetime.utcnow().isoformat(),
                        "topic": topic,
                        "pipeline_version": "1.0.0",
                    },
                }

                # Send to Kafka
                future = producer.send(
                    topic, value=enriched_record, key=str(record.get("id", published_count))
                )

                # Wait for send to complete
                record_metadata = future.get(timeout=10)

                published_count += 1

                if published_count % 100 == 0:
                    logger.info(f"Published {published_count:,} records...")

            except KafkaTimeoutError as e:
                failed_count += 1
                logger.warning(f"Failed to publish record {published_count}: {e}")
                continue

            except Exception as e:
                failed_count += 1
                logger.warning(f"Error publishing record: {e}")
                continue

        # Ensure all messages are sent
        producer.flush()

        logger.info(
            f"Kafka publish completed: {published_count:,} published, {failed_count:,} failed"
        )

        if published_count == 0 and len(data) > 0:
            raise KafkaPublishError("Failed to publish any records")

        return published_count

    except FileNotFoundError as e:
        raise KafkaPublishError(f"Data file not found: {input_path}") from e

    except KafkaError as e:
        raise KafkaPublishError(f"Kafka error: {e}") from e

    except Exception as e:
        raise KafkaPublishError(f"Publishing failed: {e}") from e

    finally:
        if producer:
            producer.close()
            logger.info("Kafka producer closed")


if __name__ == "__main__":
    try:
        count = publish_to_kafka()
        logger.info(f"Successfully published {count} records to Kafka")
        sys.exit(0)
    except KafkaPublishError as e:
        logger.error(f"Kafka publish failed: {e}")
        sys.exit(1)
