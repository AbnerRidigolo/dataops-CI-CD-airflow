.PHONY: setup up down logs ingest transform validate test lint docs clean trigger-dag

PROJECT_DIR := $(shell pwd)
DC := docker compose

# ---------- Setup ----------
setup: ## Full bootstrap: env, deps, docker, GE init, seed MinIO
	@echo "==> Copying .env.example to .env (if not exists)..."
	@cp -n .env.example .env 2>/dev/null || true
	@echo "==> Installing Python dependencies..."
	pip install -r requirements.txt -r requirements-dev.txt
	@echo "==> Starting Docker services..."
	$(DC) up -d --build
	@echo "==> Waiting for services to be healthy..."
	sleep 15
	@echo "==> Initializing Great Expectations..."
	python scripts/bootstrap_ge.py
	@echo "==> Seeding MinIO with sample data..."
	python scripts/seed_minio.py
	@echo "==> Setup complete!"

# ---------- Docker ----------
up: ## Start all services in background
	$(DC) up -d

down: ## Stop all services
	$(DC) down

logs: ## Tail scheduler logs
	$(DC) logs -f airflow-scheduler

# ---------- Data Pipeline ----------
ingest: ## Run ingestion via spark-submit in the Spark container
	$(DC) exec spark-master spark-submit \
		--master local[*] \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/airflow/ingestion/ingest_orders.py \
		--date $(shell date +%Y-%m-%d) \
		--source-path s3a://raw/olist_orders_dataset.csv \
		--dest-path s3a://bronze/orders

transform: ## Run Silver and Gold transformations
	$(DC) exec spark-master spark-submit \
		--master local[*] \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/airflow/transformations/silver_orders.py
	$(DC) exec spark-master spark-submit \
		--master local[*] \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/airflow/transformations/gold_fct_orders.py

validate: ## Run all Great Expectations checkpoints
	python scripts/bootstrap_ge.py --run-checkpoints

# ---------- Quality ----------
test: ## Run pytest with verbose output
	pytest tests/ -v --tb=short

lint: ## Run black, flake8, and mypy
	black --check .
	flake8 dags/ ingestion/ transformations/ tests/ scripts/
	mypy dags/ ingestion/ transformations/ --ignore-missing-imports

docs: ## Build and open GE Data Docs
	great_expectations docs build --no-view
	@echo "Data Docs available at http://localhost:9001 in the 'docs' bucket"

# ---------- Housekeeping ----------
clean: ## Stop containers, remove volumes, clean caches
	$(DC) down -v
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "==> Cleaned."

trigger-dag: ## Trigger the dataops_pipeline DAG via Airflow CLI
	$(DC) exec airflow-webserver airflow dags trigger dataops_pipeline

# ---------- Help ----------
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
