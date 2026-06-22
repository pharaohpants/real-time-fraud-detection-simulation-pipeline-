.PHONY: help up down restart logs test lint build clean status

# Default target
help: ## Show this help message
	@echo.
	@echo  Fraud Detection Pipeline - Available Commands
	@echo  ==================================================
	@echo.
	@echo  up          Start all services
	@echo  down        Stop all services
	@echo  restart     Restart all services
	@echo  build       Build Docker images
	@echo  logs        Follow all service logs
	@echo  status      Show service status
	@echo  test        Run unit tests locally
	@echo  lint        Run linter (flake8)
	@echo  validate    Run data quality checks
	@echo  dbt-run     Run dbt models
	@echo  dbt-test    Run dbt tests
	@echo  clean       Remove volumes and rebuild
	@echo.

# Docker operations
up: ## Start all services in detached mode
	docker-compose up -d
	@echo.
	@echo  Services started! Access:
	@echo    Grafana:  http://localhost:3000  (admin/admin)
	@echo    Airflow:  http://localhost:8080  (airflow/airflow)
	@echo    Postgres: localhost:5432

down: ## Stop all services
	docker-compose down

restart: ## Restart all services
	docker-compose down
	docker-compose up -d

build: ## Build all Docker images
	docker-compose build --no-cache

logs: ## Follow logs of all services
	docker-compose logs -f

logs-simulator: ## Follow simulator logs
	docker-compose logs -f simulator

logs-spark: ## Follow Spark logs
	docker-compose logs -f spark

status: ## Show running services
	docker-compose ps

# Development
test: ## Run unit tests locally
	pytest tests/ -v --tb=short

test-cov: ## Run tests with coverage report
	pytest tests/ -v --cov=simulator --cov=spark --cov-report=term-missing

lint: ## Run flake8 linter
	flake8 simulator/ spark/ great_expectations/ --max-line-length=120 --statistics

# Data operations
validate: ## Run data quality checks (both layers)
	docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/great_expectations && python data_quality_checks.py"

validate-raw: ## Run raw layer validation
	docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/great_expectations && python data_quality_checks.py --layer raw"

validate-gold: ## Run gold layer validation
	docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/great_expectations && python data_quality_checks.py --layer gold"

dbt-run: ## Run all dbt models
	docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt run"

dbt-test: ## Run dbt tests
	docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt test"

dbt-docs: ## Generate dbt documentation
	docker-compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt && dbt docs generate"

# Cleanup
clean: ## Remove all containers, volumes, and rebuild
	docker-compose down -v
	docker-compose build --no-cache
	@echo Cleaned and rebuilt all services.
