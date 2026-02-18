"""
Storage Module - MinIO Object Storage
=====================================
Stores processed data in MinIO (S3-compatible storage).

Author: William BATCHAYON
Version: 1.0.0
"""

import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

from minio import Minio
from minio.error import S3Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Custom exception for storage errors."""
    pass


def store_to_minio(
    input_path: str = "/tmp/kafka_output",
    endpoint: str = "minio:9000",
    access_key: str = None,
    secret_key: str = None,
    bucket_name: str = "data-bucket",
    object_prefix: str = "data",
    secure: bool = False
) -> int:
    """
    Stores JSON files from Kafka output in MinIO.
    
    Args:
        input_path: Path to the Kafka output directory
        endpoint: MinIO server endpoint
        access_key: MinIO access key
        secret_key: MinIO secret key
        bucket_name: Name of the MinIO bucket
        object_prefix: Prefix for stored objects
        secure: Use HTTPS connection
        
    Returns:
        Number of files uploaded
        
    Raises:
        StorageError: If storage fails
    """
    logger.info(f"Starting MinIO storage: {input_path}")
    
    # Get credentials from environment if not provided
    if not access_key:
        access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    if not secret_key:
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    
    client = None
    try:
        # Initialize MinIO client
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        
        logger.info(f"Connected to MinIO at {endpoint}")
        
        # Check if bucket exists, create if not
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Using existing bucket: {bucket_name}")
        
        # Check if input path exists
        if not os.path.exists(input_path):
            raise StorageError(f"Input path does not exist: {input_path}")
        
        # Find all JSON files
        json_files = []
        for root, dirs, files in os.walk(input_path):
            for file in files:
                if file.endswith('.json') or file.endswith('.parquet'):
                    json_files.append(os.path.join(root, file))
        
        if not json_files:
            logger.warning(f"No JSON files found in {input_path}")
            return 0
        
        logger.info(f"Found {len(json_files)} files to upload")
        
        # Upload each file
        uploaded_count = 0
        today = datetime.utcnow().strftime("%Y/%m/%d")
        
        for file_path in json_files:
            try:
                # Create object name with date prefix
                file_name = os.path.basename(file_path)
                object_name = f"{object_prefix}/{today}/{file_name}"
                
                # Upload the file
                client.fput_object(
                    bucket_name,
                    object_name,
                    file_path
                )
                
                uploaded_count += 1
                logger.info(f"Uploaded: {object_name}")
                
            except S3Error as e:
                logger.error(f"Failed to upload {file_path}: {e}")
                continue
        
        logger.info(f"MinIO storage completed: {uploaded_count} files uploaded")
        
        if uploaded_count == 0:
            raise StorageError("Failed to upload any files")
        
        return uploaded_count
        
    except S3Error as e:
        raise StorageError(f"MinIO error: {e}") from e
        
    except Exception as e:
        raise StorageError(f"Storage failed: {e}") from e
        
    finally:
        if client:
            logger.info("MinIO client closed")


if __name__ == "__main__":
    try:
        count = store_to_minio()
        logger.info(f"Successfully stored {count} files in MinIO")
        sys.exit(0)
    except StorageError as e:
        logger.error(f"MinIO storage failed: {e}")
        sys.exit(1)
