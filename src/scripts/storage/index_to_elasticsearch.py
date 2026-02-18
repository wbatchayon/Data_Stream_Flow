"""
Search Module - Elasticsearch Indexing
=====================================
Indexes processed data in Elasticsearch for search and analytics.

Author: William BATCHAYON
Version: 1.0.0
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, ElasticsearchException, NotFoundError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class IndexingError(Exception):
    """Custom exception for indexing errors."""

    pass


def index_to_elasticsearch(
    input_path: str = "/tmp/kafka_output",
    hosts: List[str] = None,
    index_name: str = "data_index",
    doc_type: str = "_doc",
) -> int:
    """
    Indexes JSON files from Kafka output into Elasticsearch.

    Args:
        input_path: Path to the Kafka output directory
        hosts: List of Elasticsearch hosts
        index_name: Name of the Elasticsearch index
        doc_type: Document type (for ES < 7.x compatibility)

    Returns:
        Number of documents indexed

    Raises:
        IndexingError: If indexing fails
    """
    logger.info(f"Starting Elasticsearch indexing: {input_path}")

    if not hosts:
        hosts = [os.getenv("ELASTICSEARCH_HOSTS", "http://elasticsearch:9200")]

    es = None
    try:
        # Initialize Elasticsearch client
        es = Elasticsearch(hosts, retry_on_timeout=True, max_retries=3, request_timeout=30)

        # Check connection
        if not es.ping():
            raise IndexingError("Cannot connect to Elasticsearch")

        logger.info(f"Connected to Elasticsearch at {hosts}")

        # Check if input path exists
        if not os.path.exists(input_path):
            raise IndexingError(f"Input path does not exist: {input_path}")

        # Find all JSON files
        json_files = []
        for root, dirs, files in os.walk(input_path):
            for file in files:
                if file.endswith(".json"):
                    json_files.append(os.path.join(root, file))

        if not json_files:
            logger.warning(f"No JSON files found in {input_path}")
            return 0

        logger.info(f"Found {len(json_files)} files to index")

        # Index each file
        indexed_count = 0

        for file_path in json_files:
            try:
                with open(file_path, "r") as f:
                    # Handle both single JSON and JSONL (JSON Lines)
                    content = f.read().strip()
                    if not content:
                        continue

                    # Try to parse as JSON array
                    try:
                        data = json.loads(content)
                        if not isinstance(data, list):
                            data = [data]
                    except json.JSONDecodeError:
                        # Try JSONL format
                        data = []
                        for line in content.split("\n"):
                            if line.strip():
                                data.append(json.loads(line))

                    # Index each document
                    for doc in data:
                        # Add metadata
                        doc["@timestamp"] = datetime.utcnow().isoformat()
                        doc["indexed_at"] = datetime.utcnow().isoformat()

                        # Index the document
                        es.index(index=index_name, document=doc)

                        indexed_count += 1

                logger.info(f"Indexed: {os.path.basename(file_path)}")

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON from {file_path}: {e}")
                continue

            except ElasticsearchException as e:
                logger.error(f"Elasticsearch error indexing {file_path}: {e}")
                continue

        # Refresh index to make documents searchable
        es.indices.refresh(index=index_name)

        # Get index stats
        stats = es.indices.stats(index=index_name)
        total_docs = stats["_all"]["primaries"]["docs"]["count"]

        logger.info(
            f"Elasticsearch indexing completed: {indexed_count} documents indexed, {total_docs} total in index"
        )

        return indexed_count

    except ConnectionError as e:
        raise IndexingError(f"Cannot connect to Elasticsearch: {e}") from e

    except ElasticsearchException as e:
        raise IndexingError(f"Elasticsearch error: {e}") from e

    except Exception as e:
        raise IndexingError(f"Indexing failed: {e}") from e

    finally:
        if es:
            logger.info("Elasticsearch client closed")


if __name__ == "__main__":
    try:
        count = index_to_elasticsearch()
        logger.info(f"Successfully indexed {count} documents in Elasticsearch")
        sys.exit(0)
    except IndexingError as e:
        logger.error(f"Elasticsearch indexing failed: {e}")
        sys.exit(1)
