# Data Stream Flow - Deployment Guide

## Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- Git

## Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/batchayw/Data_Stream_Flow.git
cd Data_Stream_Flow
cp .env.example .env
```

### 2. Configure Environment

Edit `.env` with your settings:

```bash
# Required configurations
AIRFLOW_DB_PASSWORD=your_secure_password
MINIO_ROOT_USER=your_minio_user
MINIO_ROOT_PASSWORD=your_minio_password
AIRFLOW_WEBSERVER_SECRET_KEY=your_secret_key

# Optional: Email notifications
SMTP_HOST=smtp.gmail.com
SMTP_USER=your_email
SMTP_PASSWORD=your_app_password
```

### 3. Start Services

```bash
# Development
make dev-up

# Production
make prod-up
```

### 4. Verify Deployment

```bash
make health-check
```

## Development Deployment

### Services Access
| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin/admin |
| MinIO | http://localhost:9000 | minioadmin/minioadmin |
| Kibana | http://localhost:5601 | - |
| API | http://localhost:5000 | - |
| Grafana | http://localhost:3000 | admin/admin |

### Common Commands

```bash
# View logs
make logs

# Run tests
make test-unit

# Stop services
make dev-down
```

## Production Deployment

### 1. Security Hardening

1. Change default passwords
2. Enable TLS/SSL
3. Configure firewall rules
4. Set up monitoring alerts

### 2. Resource Planning

Recommended resources:
- Airflow: 2 CPU, 2GB RAM
- PostgreSQL: 1 CPU, 1GB RAM
- Kafka: 1 CPU, 2GB RAM
- MinIO: 1 CPU, 1GB RAM
- Elasticsearch: 1 CPU, 1GB RAM

### 3. Backup Strategy

```bash
# Backup PostgreSQL
docker compose exec postgres pg_dump -U airflow airflow > backup.sql

# Backup MinIO data
docker compose exec minio mc mirror /data backup/
```

### 4. Health Checks

```bash
# Check all services
docker compose ps

# Check service logs
docker compose logs -f airflow
```

## Kubernetes Deployment

For production Kubernetes deployment, see `infra/kubernetes/` directory.

## Troubleshooting

### Services won't start
- Check Docker logs: `docker compose logs`
- Verify `.env` configuration
- Check port conflicts

### DAG not loading
- Verify DAG file syntax: `docker compose exec airflow python -c "import sys; sys.path.append('/opt/airflow/dags'); import data_pipeline_dag"`

### Connection issues
- Check network: `docker network ls`
- Verify service names in configuration

## Maintenance

### Update Services

```bash
# Pull latest images
docker compose pull

# Restart services
docker compose up -d
```

### Clean Up

```bash
# Remove unused resources
docker system prune

# Full reset
make reset
```

## Support

- Open issues for bugs
- Check logs for error details
- Review documentation in `docs/`
