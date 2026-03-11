# ─────────────────────────────────────────────────────────────────────────────
# IoT Telemetry Pipeline — Task Runner
# ─────────────────────────────────────────────────────────────────────────────
#
# Usage:
#   make help            Show this help
#   make generator       Start the synthetic data generator
#   make bronze          Start the Bronze streaming job
#   make silver          Start the Silver streaming job
#   make gold            Run the Gold batch aggregation
#   make pipeline        Run generator + Bronze + Silver + Gold end-to-end
#   make dashboard       Full pipeline + HTML observability dashboard
#   make test            Run the full test suite (142 tests)
#   make test-unit       Run only fast unit tests
#   make test-integration Run only cross-layer integration tests
#   make validate        Run Bronze, Silver, Gold validation scripts
#   make clean           Delete all generated data and checkpoints
#   make install         Install Python dependencies
#   make docker-up       Start the Spark cluster in Docker
#   make docker-down     Tear down the Docker cluster
#
# ─────────────────────────────────────────────────────────────────────────────

.DEFAULT_GOAL := help
SHELL := /bin/bash

PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest

DATA_DIR     := data
DOCKER_DIR   := infra/docker
COMPOSE_FILE := $(DOCKER_DIR)/docker-compose.yml


# ── Pipeline Jobs ────────────────────────────────────────────────────────────

.PHONY: generator bronze silver gold pipeline dashboard

generator:  ## Start the synthetic IoT telemetry generator
	$(PYTHON) -m generator.iot_telemetry_generator

bronze:  ## Start the Bronze streaming ingestion job
	$(PYTHON) -m pipeline.bronze_ingest.bronze_ingest_stream

silver:  ## Start the Silver transformation streaming job
	$(PYTHON) -m pipeline.silver_transform.silver_transform_job

gold:  ## Run the Gold batch aggregation
	$(PYTHON) -m pipeline.gold_aggregations.gold_aggregations_job

pipeline:  ## Run full pipeline: generate data -> Bronze -> Silver -> Gold
	$(PYTHON) scripts/generate_dashboard.py

dashboard:  ## Generate the HTML observability dashboard
	$(PYTHON) scripts/generate_dashboard.py


# ── Validation ───────────────────────────────────────────────────────────────

.PHONY: validate validate-bronze validate-silver validate-gold governance e2e resilience

validate: validate-bronze validate-silver validate-gold  ## Run all layer validation scripts

governance:  ## Run deterministic data-quality governance test
	$(PYTHON) scripts/validate_governance.py

e2e:  ## Run end-to-end integration test (generator -> Bronze -> Silver -> Gold)
	$(PYTHON) scripts/validate_e2e.py

resilience:  ## Run operational resilience tests (restart, backpressure, late data, drift)
	$(PYTHON) scripts/validate_resilience.py

validate-bronze:  ## Validate the Bronze Delta table
	$(PYTHON) scripts/validate_bronze.py

validate-silver:  ## Validate the Silver Delta table
	$(PYTHON) scripts/validate_silver.py

validate-gold:  ## Validate the Gold Delta tables
	$(PYTHON) scripts/validate_gold.py


# ── Testing ──────────────────────────────────────────────────────────────────

.PHONY: test test-unit test-integration test-fast

test:  ## Run the full test suite
	$(PYTEST) tests/ -v

test-unit:  ## Run only unit tests (fast)
	$(PYTEST) tests/ -v -m unit

test-integration:  ## Run only cross-layer integration tests
	$(PYTEST) tests/ -v -m integration

test-fast:  ## Run all tests except slow ones
	$(PYTEST) tests/ -v -m "not slow"


# ── Environment ──────────────────────────────────────────────────────────────

.PHONY: install clean clean-checkpoints

install:  ## Install Python dependencies
	$(PYTHON) -m pip install -r requirements.txt

clean:  ## Delete all generated data, checkpoints, and caches
	rm -rf $(DATA_DIR)
	rm -rf .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned: $(DATA_DIR)/, .pytest_cache/, __pycache__/"

clean-checkpoints:  ## Delete only streaming checkpoints (keeps Delta tables)
	rm -rf $(DATA_DIR)/delta/checkpoints
	@echo "Cleaned: $(DATA_DIR)/delta/checkpoints/"


# ── Docker ───────────────────────────────────────────────────────────────────

.PHONY: docker-up docker-down docker-build docker-logs docker-pipeline docker-test

docker-build:  ## Build the Docker images
	docker compose -f $(COMPOSE_FILE) build

docker-up:  ## Start the Spark cluster (master + worker + Jupyter)
	docker compose -f $(COMPOSE_FILE) up -d spark-master spark-worker jupyter

docker-down:  ## Tear down the entire Docker stack
	docker compose -f $(COMPOSE_FILE) down -v

docker-logs:  ## Tail logs from all running containers
	docker compose -f $(COMPOSE_FILE) logs -f

docker-pipeline:  ## Run the full pipeline inside Docker
	docker compose -f $(COMPOSE_FILE) run --rm pipeline

docker-test:  ## Run the test suite inside Docker
	docker compose -f $(COMPOSE_FILE) run --rm pipeline test


# ── Help ─────────────────────────────────────────────────────────────────────

.PHONY: help

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
