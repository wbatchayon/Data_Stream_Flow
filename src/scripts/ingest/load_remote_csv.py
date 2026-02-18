"""
Data Ingestion Module - Load Remote CSV
=======================================
Downloads CSV data from remote sources with proper logging and error handling.

Author: William BATCHAYON
Version: 1.0.0
"""

import logging
import os
import sys
from typing import Optional

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DataIngestionError(Exception):
    """Custom exception for data ingestion errors."""

    pass


def load_remote_csv(
    url: str = "https://raw.githubusercontent.com/batchayw/tech-data-analysis/main/broadband_data_zipcode.csv",
    output_path: str = "/tmp/remote_data.csv",
    max_retries: int = 3,
    timeout: int = 60,
) -> str:
    """
    Downloads a CSV file from a remote URL with retry logic.

    Args:
        url: URL of the remote CSV file
        output_path: Local path to save the file
        max_retries: Maximum number of retry attempts
        timeout: Request timeout in seconds

    Returns:
        Path to the downloaded file

    Raises:
        DataIngestionError: If the download fails after all retries
    """
    logger.info(f"Starting data ingestion from: {url}")

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Download attempt {attempt}/{max_retries}")

            response = requests.get(
                url, timeout=timeout, headers={"User-Agent": "DataStreamFlow/1.0"}
            )
            response.raise_for_status()

            # Check for empty response
            if len(response.content) == 0:
                raise DataIngestionError("Received empty response from server")

            # Save the file
            with open(output_path, "wb") as f:
                f.write(response.content)

            file_size = os.path.getsize(output_path)
            logger.info(f"Successfully downloaded {file_size:,} bytes to {output_path}")

            return output_path

        except requests.exceptions.Timeout as e:
            logger.warning(f"Request timeout on attempt {attempt}: {e}")
            if attempt == max_retries:
                raise DataIngestionError(
                    f"Download failed after {max_retries} attempts: Timeout"
                ) from e

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error on attempt {attempt}: {e}")
            if attempt == max_retries:
                raise DataIngestionError(
                    f"Download failed after {max_retries} attempts: {e}"
                ) from e

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error on attempt {attempt}: {e}")
            if attempt == max_retries:
                raise DataIngestionError(
                    f"Download failed after {max_retries} attempts: {e}"
                ) from e

        except IOError as e:
            logger.error(f"IO error on attempt {attempt}: {e}")
            raise DataIngestionError(f"Failed to write file: {e}") from e

    raise DataIngestionError("Download failed: Unknown error")


def validate_csv(file_path: str, min_rows: int = 1) -> bool:
    """
    Validates that a CSV file exists and has data.

    Args:
        file_path: Path to the CSV file
        min_rows: Minimum number of rows required

    Returns:
        True if valid, False otherwise
    """
    if not os.path.exists(file_path):
        logger.error(f"CSV file not found: {file_path}")
        return False

    file_size = os.path.getsize(file_path)
    if file_size == 0:
        logger.error(f"CSV file is empty: {file_path}")
        return False

    logger.info(f"CSV validation passed: {file_path} ({file_size:,} bytes)")
    return True


if __name__ == "__main__":
    try:
        result = load_remote_csv()
        if validate_csv(result):
            logger.info("Data ingestion completed successfully")
            sys.exit(0)
        else:
            logger.error("Data validation failed")
            sys.exit(1)
    except DataIngestionError as e:
        logger.error(f"Data ingestion failed: {e}")
        sys.exit(1)
