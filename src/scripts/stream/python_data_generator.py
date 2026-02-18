"""
Data Generator Module - Python Data Generator
==============================================
Generates additional data based on processed CSV data.

Author: William BATCHAYON
Version: 1.0.0
"""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataGenerationError(Exception):
    """Custom exception for data generation errors."""
    pass


def python_data_generator(
    input_path: str = "/tmp/spark_processed.csv",
    output_path: str = "/tmp/generated_data.json"
) -> str:
    """
    Generates additional data based on processed CSV data.
    
    Args:
        input_path: Path to the processed CSV file
        output_path: Path to save the generated JSON data
        
    Returns:
        Path to the generated data file
        
    Raises:
        DataGenerationError: If generation fails
    """
    logger.info(f"Starting data generation from: {input_path}")
    
    try:
        # Load the processed CSV file
        # Try to find the CSV file (may be in subdirectory due to Spark output)
        if not os.path.exists(input_path):
            csv_dir = os.path.dirname(input_path)
            if os.path.exists(csv_dir):
                for f in os.listdir(csv_dir):
                    if f.endswith('.csv') and not f.startswith('_'):
                        input_path = os.path.join(csv_dir, f)
                        break
        
        if not os.path.exists(input_path):
            raise DataGenerationError(f"Input file not found: {input_path}")
        
        df = pd.read_csv(input_path)
        original_count = len(df)
        logger.info(f"Loaded {original_count:,} rows from {input_path}")
        
        # Generate additional data based on existing data
        generated_data = []
        
        for idx, row in df.iterrows():
            # Create enhanced records with generated fields
            record = {
                "id": int(row.iloc[0]) if len(row) > 0 else idx,
                "original_data": row.to_dict(),
                "generated_timestamp": datetime.utcnow().isoformat(),
                "record_type": "generated_enrichment",
                "processing_info": {
                    "source": "python_data_generator",
                    "version": "1.0.0",
                    "pipeline": "data_stream_flow"
                }
            }
            
            # Add computed fields
            if len(row) > 1:
                record["computed_hash"] = hash(tuple(row.values))
                record["record_size_bytes"] = sum(len(str(v)) for v in row.values)
            
            generated_data.append(record)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save the generated data as JSON
        with open(output_path, "w") as f:
            json.dump(generated_data, f, indent=2, default=str)
        
        output_size = os.path.getsize(output_path)
        logger.info(f"Generated {len(generated_data):,} records ({output_size:,} bytes) to {output_path}")
        
        return output_path
        
    except FileNotFoundError as e:
        raise DataGenerationError(f"Input file not found: {input_path}") from e
    except Exception as e:
        raise DataGenerationError(f"Data generation failed: {e}") from e


if __name__ == "__main__":
    try:
        result = python_data_generator()
        logger.info("Data generation completed successfully")
        sys.exit(0)
    except DataGenerationError as e:
        logger.error(f"Data generation failed: {e}")
        sys.exit(1)
