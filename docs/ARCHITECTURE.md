# Data Stream Flow - Architecture

## Overview

Data Stream Flow is an enterprise-grade data pipeline built with MLOps and DevSecOps best practices. It orchestrates the complete lifecycle of data from ingestion to visualization.

## Components

### 1. Data Ingestion
- **Remote CSV Loader**: Downloads CSV from remote URLs
- **Validation**: Ensures data quality before processing

### 2. Data Processing
- **Pandas**: Data cleaning, transformation, and preparation
- **Spark Batch**: Distributed processing for large datasets

### 3. Data Streaming
- **Spark Streaming 1**: CSV stream preparation
- **Python Data Generator**: Data enrichment and generation
- **Kafka**: Message broker for real-time streaming
- **Spark Streaming 2**: Real-time data consumption

### 4. Data Storage
- **MinIO**: S3-compatible object storage for data lake
- **Elasticsearch**: Search and analytics engine

### 5. Orchestration
- **Apache Airflow**: DAG-based workflow orchestration

### 6. Monitoring
- **Prometheus**: Metrics collection
- **Grafana**: Visualization and dashboards

## Data Flow

1. **Ingestion**: CSV downloaded from remote source
2. **Cleaning**: Pandas removes duplicates, handles missing values
3. **Processing**: Spark performs distributed computations
4. **Streaming**: Data streamed via Kafka
5. **Storage**: Processed data stored in MinIO
6. **Indexing**: Data indexed in Elasticsearch for search/analytics
7. **Visualization**: Kibana provides real-time dashboards

## Infrastructure

### Services
| Service | Port | Purpose |
|---------|------|---------|
| Airflow | 8080 | Workflow orchestration |
| PostgreSQL | 5432 | Metadata storage |
| Kafka | 9092 | Message streaming |
| Zookeeper | 2181 | Kafka coordination |
| MinIO | 9000 | Object storage |
| MinIO Console | 9001 | Storage UI |
| Elasticsearch | 9200 | Search engine |
| Kibana | 5601 | Visualization |
| API | 5000 | Pipeline trigger |
| Prometheus | 9090 | Metrics |
| Grafana | 3000 | Dashboards |

## Security

- TLS/SSL for all services
- Network segmentation via Docker networks
- Secrets management via environment variables
- Non-root container execution
- Regular security scanning (Trivy, Bandit)

## Scalability

- Horizontal scaling via container orchestration
- Spark distributed processing
- Kafka partitioning
- MinIO distributed mode

## Resilience

- Auto-restart on failure
- Retry mechanisms
- Checkpointing for streaming
- Health checks for all services
