# ============================================
# Data Stream Flow - Makefile
# ============================================

.PHONY: help dev-up dev-down prod-up prod-down build test lint security-scan clean logs reset health-check

# Default target
help:
	@echo "Data Stream Flow - Available Commands"
	@echo "======================================"
	@echo ""
	@echo "Development:"
	@echo "  make dev-up        - Start development environment"
	@echo "  make dev-down      - Stop development environment"
	@echo ""
Production:"
	@echo "  make prod-up       - Start production environment"
	@echo "	@echo "  make prod-down     - Stop production environment"
	@echo ""
	@echo "Testing:"
	@echo "  make test-unit     - Run unit tests"
	@echo "  make test-integration - Run integration tests"
	@echo "  make test-all      - Run all tests with coverage"
	@echo ""
	@echo "Code Quality:"
	@echo "  make lint          - Run code linting"
	@echo "  make format        - Format code"
	@echo "  make security-scan - Run security scans"
	@echo ""
	@echo "Utilities:"
	@echo "  make logs          - View logs"
	@echo "  make health-check  - Check service health"
	@echo "  make clean         - Clean up containers and volumes"
	@echo "  make reset         - Reset the environment"

# ============================================
# Development Commands
# ============================================

dev-up:
	@echo "Starting development environment..."
	cp -n .env.example .env || true
	docker compose up -d
	@echo "Development environment started!"
	@echo "Airflow: http://localhost:8080"
	@echo "MinIO: http://localhost:9000"
	@echo "Kibana: http://localhost:5601"
	@echo "API: http://localhost:5000"

dev-down:
	@echo "Stopping development environment..."
	docker compose down

# ============================================
# Production Commands
# ============================================

prod-up:
	@echo "Starting production environment..."
	@if [ ! -f .env ]; then cp .env.example .env; fi
	docker compose -f docker-compose.yml up -d --build
	@echo "Production environment started!"

prod-down:
	@echo "Stopping production environment..."
	docker compose -f docker-compose.yml down

# ============================================
# Build Commands
# ============================================

build:
	@echo "Building Docker images..."
	docker compose build --no-cache

build-dev:
	@echo "Building development images..."
	docker compose build

# ============================================
# Testing Commands
# ============================================

test-unit:
	@echo "Running unit tests..."
	docker compose run --rm airflow pytest tests/unit/ -v

test-integration:
	@echo "Running integration tests..."
	docker compose run --rm airflow pytest tests/integration/ -v

test-e2e:
	@echo "Running E2E tests..."
	docker compose run --rm airflow pytest tests/e2e/ -v

test-all:
	@echo "Running all tests with coverage..."
	docker compose run --rm airflow pytest tests/ -v --cov=src --cov=scripts --cov-report=html

test:
	@echo "Running tests..."
	pytest tests/ -v

# ============================================
# Code Quality Commands
# ============================================

lint:
	@echo "Running linting..."
	flake8 src/ scripts/ tests/ --count --select=E9,F63,F7,F82 --show-source --statistics
	black --check src/ scripts/ tests/
	isort --check-only --diff src/ scripts/ tests/

format:
	@echo "Formatting code..."
	black src/ scripts/ tests/
	isort src/ scripts/ tests/

security-scan:
	@echo "Running security scans..."
	@echo "Running Bandit..."
	bandit -r src/ scripts/ -f txt
	@echo "Running Trivy..."
	trivy fs --security-checks vuln,config .

# ============================================
# Utility Commands
# ============================================

logs:
	docker compose logs -f

logs-airflow:
	docker compose logs -f airflow

logs-kafka:
	docker compose logs -f kafka

logs-elasticsearch:
	docker compose logs -f elasticsearch

health-check:
	@echo "Checking service health..."
	@echo "Airflow: $$(curl -sf http://localhost:8080/health | jq -r '.status' 2>/dev/null || echo 'not running')"
	@echo "MinIO: $$(curl -sf http://localhost:9000/minio/health/live | jq -r '.status' 2>/dev/null || echo 'not running')"
	@echo "Elasticsearch: $$(curl -sf http://localhost:9200 | jq -r '.version.number' 2>/dev/null || echo 'not running')"

clean:
	@echo "Cleaning up..."
	docker compose down -v --remove-orphans
	rm -rf __pycache__ src/__pycache__ tests/__pycache__
	rm -rf .pytest_cache .coverage htmlcov
	rm -rf *.log

reset: clean
	@echo "Reset complete. Run 'make dev-up' to start fresh."

# ============================================
# Database Commands
# ============================================

db-init:
	@echo "Initializing Airflow database..."
	docker compose exec airflow airflow db init

db-migrate:
	@echo "Running Airflow migrations..."
	docker compose exec airflow airflow db migrate

# ============================================
# DAG Commands
# ============================================

dags-list:
	docker compose exec airflow airflow dags list

dags-trigger:
	docker compose exec airflow airflow dags trigger data_pipeline_dag

dags-unpause:
	docker compose exec airflow airflow dags unpause data_pipeline_dag
