"""
Data Pipeline DAG - Apache Airflow Orchestration
===============================================
Orchestrates the complete data processing pipeline.

Author: William BATCHAYON
Version: 1.0.0
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add scripts to Python path
SCRIPTS_PATH = "/opt/airflow/scripts"
if SCRIPTS_PATH not in sys.path:
    sys.path.append(SCRIPTS_PATH)

# Import pipeline modules
try:
    from scripts.ingest.load_remote_csv import load_remote_csv, validate_csv
    from scripts.process.process_with_pandas_and_spark import process_with_pandas_and_spark
    from scripts.stream.spark_streaming_1 import spark_streaming_1
    from scripts.stream.python_data_generator import python_data_generator
    from scripts.stream.publish_to_kafka import publish_to_kafka
    from scripts.stream.spark_streaming_2 import spark_streaming_2
    from scripts.storage.store_to_minio import store_to_minio
    from scripts.storage.index_to_elasticsearch import index_to_elasticsearch
except ImportError as e:
    logger.warning(f"Failed to import scripts: {e}")
    # Fallback to direct imports
    pass

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [os.getenv('AIRFLOW__SMTP__SMTP_USER', 'admin@example.com')],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# DAG configuration
DAG_ID = 'data_pipeline_dag'
SCHEDULE_INTERVAL = os.getenv('PIPELINE_SCHEDULE_INTERVAL', '@hourly')
START_DATE = datetime(2025, 1, 1)


def failure_callback(context):
    """Callback function when a task fails."""
    logger.error(f"Task failed: {context.get('task_instance_key_str')}")
    # Additional alerting can be added here


# Define the DAG
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Data Stream Flow - Complete data processing pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    tags=['data-pipeline', 'mlops', 'streaming'],
    on_failure_callback=failure_callback,
) as dag:
    
    # ============================================
    # Pipeline Start
    # ============================================
    pipeline_start = EmptyOperator(
        task_id='pipeline_start',
        dag=dag
    )
    
    # ============================================
    # Stage 1: Data Ingestion
    # ============================================
    with TaskGroup('ingestion_group') as ingestion_group:
        load_csv = PythonOperator(
            task_id='load_remote_csv',
            python_callable=load_remote_csv,
            op_kwargs={
                'url': os.getenv('DATA_SOURCE_URL', 'https://raw.githubusercontent.com/batchayw/tech-data-analysis/main/broadband_data_zipcode.csv'),
                'output_path': '/tmp/remote_data.csv'
            },
            dag=dag
        )
        
        validate_data = PythonOperator(
            task_id='validate_csv',
            python_callable=validate_csv,
            op_kwargs={
                'file_path': '/tmp/remote_data.csv',
                'min_rows': 1
            },
            dag=dag
        )
        
        load_csv >> validate_data
    
    # ============================================
    # Stage 2: Data Processing
    # ============================================
    with TaskGroup('processing_group') as processing_group:
        process_data = PythonOperator(
            task_id='process_with_pandas_and_spark',
            python_callable=process_with_pandas_and_spark,
            op_kwargs={
                'input_csv': '/tmp/remote_data.csv',
                'pandas_output': '/tmp/pandas_processed.csv',
                'spark_output': '/tmp/spark_processed.csv'
            },
            dag=dag
        )
        
        process_data
    
    # ============================================
    # Stage 3: Streaming Preparation
    # ============================================
    with TaskGroup('streaming_prep_group') as streaming_prep_group:
        stream_prep = PythonOperator(
            task_id='spark_streaming_1',
            python_callable=spark_streaming_1,
            op_kwargs={
                'input_path': '/tmp/spark_processed.csv',
                'processing_time': 30
            },
            dag=dag
        )
        
        generate_data = PythonOperator(
            task_id='python_data_generator',
            python_callable=python_data_generator,
            op_kwargs={
                'input_path': '/tmp/spark_processed.csv',
                'output_path': '/tmp/generated_data.json'
            },
            dag=dag
        )
        
        stream_prep >> generate_data
    
    # ============================================
    # Stage 4: Kafka Publishing
    # ============================================
    with TaskGroup('kafka_group') as kafka_group:
        publish_to_kafka = PythonOperator(
            task_id='publish_to_kafka',
            python_callable=publish_to_kafka,
            op_kwargs={
                'input_path': '/tmp/generated_data.json',
                'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
                'topic': os.getenv('KAFKA_TOPIC', 'data_topic')
            },
            dag=dag
        )
        
        kafka_consume = PythonOperator(
            task_id='spark_streaming_2',
            python_callable=spark_streaming_2,
            op_kwargs={
                'kafka_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
                'topic': os.getenv('KAFKA_TOPIC', 'data_topic'),
                'output_path': '/tmp/kafka_output',
                'processing_time': 30
            },
            dag=dag
        )
        
        publish_to_kafka >> kafka_consume
    
    # ============================================
    # Stage 5: Storage and Indexing
    # ============================================
    with TaskGroup('storage_group') as storage_group:
        store_minio = PythonOperator(
            task_id='store_to_minio',
            python_callable=store_to_minio,
            op_kwargs={
                'input_path': '/tmp/kafka_output',
                'endpoint': os.getenv('MINIO_ENDPOINT', 'minio:9000'),
                'bucket_name': os.getenv('MINIO_BUCKET', 'data-bucket')
            },
            dag=dag
        )
        
        index_es = PythonOperator(
            task_id='index_to_elasticsearch',
            python_callable=index_to_elasticsearch,
            op_kwargs={
                'input_path': '/tmp/kafka_output',
                'index_name': os.getenv('ES_INDEX', 'data_index')
            },
            dag=dag
        )
        
        store_minio >> index_es
    
    # ============================================
    # Pipeline End
    # ============================================
    pipeline_end = EmptyOperator(
        task_id='pipeline_end',
        dag=dag
    )
    
    # ============================================
    # Define Dependencies
    # ============================================
    pipeline_start >> ingestion_group
    ingestion_group >> processing_group
    processing_group >> streaming_prep_group
    streaming_prep_group >> kafka_group
    kafka_group >> storage_group
    storage_group >> pipeline_end
