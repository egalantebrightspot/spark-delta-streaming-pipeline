@echo off
REM ── Windows task runner — mirrors the Makefile targets ──────────────────
REM Usage:  make <target>
REM         make help
REM
REM If GNU make is available, prefer: make <target>
REM This wrapper exists for environments without GNU make.

setlocal

if "%~1"=="" goto :help
goto :%~1 2>nul || (
    echo Unknown target: %~1
    echo Run "make help" to see available targets.
    exit /b 1
)

:generator
    python -m generator.iot_telemetry_generator
    goto :eof

:bronze
    python -m pipeline.bronze_ingest.bronze_ingest_stream
    goto :eof

:silver
    python -m pipeline.silver_transform.silver_transform_job
    goto :eof

:gold
    python -m pipeline.gold_aggregations.gold_aggregations_job
    goto :eof

:pipeline
    python scripts\generate_dashboard.py
    goto :eof

:dashboard
    python scripts\generate_dashboard.py
    goto :eof

:validate
    python scripts\validate_bronze.py
    python scripts\validate_silver.py
    python scripts\validate_gold.py
    goto :eof

:validate-bronze
    python scripts\validate_bronze.py
    goto :eof

:validate-silver
    python scripts\validate_silver.py
    goto :eof

:validate-gold
    python scripts\validate_gold.py
    goto :eof

:test
    python -m pytest tests/ -v
    goto :eof

:test-unit
    python -m pytest tests/ -v -m unit
    goto :eof

:test-integration
    python -m pytest tests/ -v -m integration
    goto :eof

:test-fast
    python -m pytest tests/ -v -m "not slow"
    goto :eof

:install
    python -m pip install -r requirements.txt
    goto :eof

:clean
    if exist data rmdir /s /q data
    if exist .pytest_cache rmdir /s /q .pytest_cache
    for /d /r %%d in (__pycache__) do if exist "%%d" rmdir /s /q "%%d"
    echo Cleaned: data/, .pytest_cache/, __pycache__/
    goto :eof

:clean-checkpoints
    if exist data\delta\checkpoints rmdir /s /q data\delta\checkpoints
    echo Cleaned: data/delta/checkpoints/
    goto :eof

:docker-build
    docker compose -f infra\docker\docker-compose.yml build
    goto :eof

:docker-up
    docker compose -f infra\docker\docker-compose.yml up -d spark-master spark-worker jupyter
    goto :eof

:docker-down
    docker compose -f infra\docker\docker-compose.yml down -v
    goto :eof

:docker-logs
    docker compose -f infra\docker\docker-compose.yml logs -f
    goto :eof

:docker-pipeline
    docker compose -f infra\docker\docker-compose.yml run --rm pipeline
    goto :eof

:docker-test
    docker compose -f infra\docker\docker-compose.yml run --rm pipeline test
    goto :eof

:help
    echo.
    echo   IoT Telemetry Pipeline - Task Runner
    echo   =====================================
    echo.
    echo   Pipeline:
    echo     make generator          Start the synthetic data generator
    echo     make bronze             Start the Bronze streaming job
    echo     make silver             Start the Silver streaming job
    echo     make gold               Run the Gold batch aggregation
    echo     make pipeline           Run full pipeline end-to-end
    echo     make dashboard          Full pipeline + HTML dashboard
    echo.
    echo   Validation:
    echo     make validate           Run all validation scripts
    echo     make validate-bronze    Validate Bronze layer
    echo     make validate-silver    Validate Silver layer
    echo     make validate-gold      Validate Gold layer
    echo.
    echo   Testing:
    echo     make test               Run the full test suite (142 tests)
    echo     make test-unit          Run only unit tests (fast)
    echo     make test-integration   Run cross-layer integration tests
    echo     make test-fast          Skip slow tests
    echo.
    echo   Environment:
    echo     make install            Install Python dependencies
    echo     make clean              Delete all data, caches, checkpoints
    echo     make clean-checkpoints  Delete only streaming checkpoints
    echo.
    echo   Docker:
    echo     make docker-build       Build Docker images
    echo     make docker-up          Start Spark cluster + Jupyter
    echo     make docker-down        Tear down Docker stack
    echo     make docker-logs        Tail container logs
    echo     make docker-pipeline    Run pipeline in Docker
    echo     make docker-test        Run tests in Docker
    echo.
    goto :eof
