# Data Stream Flow

[![PyPI Version](https://img.shields.io/pypi/v/datastreamflow.svg)](https://pypi.org/project/datastreamflow/)
[![Python Versions](https://img.shields.io/pypi/pyversions/datastreamflow.svg)](https://pypi.org/project/datastreamflow/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code Style: Black](https://img.shields.io/badge/Code%20Style-Black-000000.svg)](https://github.com/psf/black)
[![CI/CD](https://github.com/batchayw/Data_Stream_Flow/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/batchayw/Data_Stream_Flow/actions/workflows/ci-cd.yml)
[![Security: Bandit](https://img.shields.io/badge/Security-Bandit-green.svg)](https://bandit.readthedocs.io/)
[![Total Downloads](https://img.shields.io/github/downloads/batchayw/Data_Stream_Flow/total.svg)](https://github.com/batchayw/Data_Stream_Flow/releases)
[![Contributors](https://img.shields.io/github/contributors/batchayw/Data_Stream_Flow.svg)](https://github.com/batchayw/Data_Stream_Flow/graphs/contributors)
[![Last Commit](https://img.shields.io/github/last-commit/batchayw/Data_Stream_Flow/main.svg)](https://github.com/batchayw/Data_Stream_Flow/commits/main)

> **Enterprise-grade data pipeline with MLOps and DevSecOps best practices**

**Data Stream Flow** is a robust, automated, and monitored data pipeline that orchestrates the complete lifecycle of data from ingestion to visualization.

## Features

- 📥 **Data Ingestion** - CSV from remote sources with validation
- 🔄 **Data Processing** - Pandas + Spark distributed processing
- 📡 **Real-time Streaming** - Kafka + Spark Streaming
- 💾 **Object Storage** - MinIO (S3-compatible)
- 🔍 **Search & Analytics** - Elasticsearch + Kibana
- ⚙️ **Orchestration** - Apache Airflow DAGs
- 📊 **Monitoring** - Prometheus + Grafana
- 🔒 **Security** - Trivy, Bandit, TruffleHog scanning
- ✅ **Testing** - Unit, Integration, and E2E tests

## Quick Start

### Installation

```bash
# From PyPI
pip install datastreamflow

# Or from source
git clone https://github.com/batchayw/Data_Stream_Flow.git
cd Data_Stream_Flow
pip install -e ".[dev]"
```

### Docker Deployment

```bash
# Clone and configure
git clone https://github.com/batchayw/Data_Stream_Flow.git
cd Data_Stream_Flow
cp .env.example .env

# Start services
docker compose up -d

# Access services
# Airflow: http://localhost:8080 (admin/admin)
# MinIO: http://localhost:9000 (minioadmin/minioadmin)
# Kibana: http://localhost:5601
# API: http://localhost:5000
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md)
- [Security](docs/SECURITY.md)
- [Deployment](docs/DEPLOYMENT.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

## Development

### Prerequisites

- Python 3.8+
- Docker and Docker Compose
- Git

### Local Development

```bash
# Clone repository
git clone https://github.com/batchayw/Data_Stream_Flow.git
cd Data_Stream_Flow

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
pip install -e ".[dev]"

# Run tests
pytest

# Start services
make dev-up
```

### Available Commands

```bash
make dev-up        # Start development environment
make dev-down      # Stop development environment
make test          # Run all tests
make lint          # Run code linting
make security-scan # Run security scans
make logs          # View logs
make clean         # Clean up
```

## CI/CD Pipeline

The pipeline includes:

1. **Lint** - Code quality checks (Flake8, Black, isort, Bandit)
2. **Security** - Vulnerability scanning (Trivy, TruffleHog, Safety)
3. **Unit Tests** - Core functionality tests
4. **Integration** - Docker build and integration tests
5. **E2E** - End-to-end pipeline tests
6. **Notify** - Slack notifications

## Technologies

| Category | Technologies |
|----------|-------------|
| Orchestration | Apache Airflow |
| Processing | Pandas, Apache Spark |
| Streaming | Apache Kafka, Spark Streaming |
| Storage | MinIO, Elasticsearch |
| Monitoring | Prometheus, Grafana |
| Security | Trivy, Bandit, TruffleHog |
| Testing | Pytest, unittest |

## Contributing

Contributions are welcome! Please see our [Contributing Guide](CONTRIBUTING.md).

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**William BATCHAYON**
- GitHub: [@batchayw](https://github.com/batchayw)
- Email: batchayw@protonmail.com

## Support

- 📖 [Documentation](docs/)
- 🐛 [Issue Tracker](https://github.com/batchayw/Data_Stream_Flow/issues)
- 💬 [Discussions](https://github.com/batchayw/Data_Stream_Flow/discussions)

---

<p align="center">
  Made with ❤️ by <a href="https://github.com/batchayw">William BATCHAYON</a>
</p>
