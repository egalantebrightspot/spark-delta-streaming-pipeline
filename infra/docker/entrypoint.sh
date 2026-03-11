#!/bin/bash
set -euo pipefail

case "${1:-help}" in
  master|worker)
    # Delegate to Bitnami's native Spark entrypoint which reads SPARK_MODE.
    exec /opt/bitnami/scripts/spark/entrypoint.sh \
         /opt/bitnami/scripts/spark/run.sh
    ;;

  generator)
    exec python -m generator.iot_telemetry_generator
    ;;

  bronze)
    exec python -m pipeline.bronze_ingest.bronze_ingest_stream
    ;;

  silver)
    exec python -m pipeline.silver_transform.silver_transform_job
    ;;

  gold)
    exec python -m pipeline.gold_aggregations.gold_aggregations_job
    ;;

  pipeline|dashboard)
    exec python scripts/generate_dashboard.py
    ;;

  jupyter)
    exec jupyter lab \
      --ip=0.0.0.0 --port=8888 --no-browser --allow-root \
      --notebook-dir=/app/notebooks
    ;;

  test)
    exec python -m pytest tests/ -v
    ;;

  help)
    cat <<'EOF'
Spark Delta Streaming Pipeline - Docker Entrypoint

Usage:  docker run <image> <command>

Cluster:
  master       Start Spark master node
  worker       Start Spark worker node

Pipeline:
  generator    Run IoT telemetry data generator
  bronze       Run Bronze ingestion stream
  silver       Run Silver transformation stream
  gold         Run Gold aggregation batch job
  pipeline     Run full pipeline (Bronze -> Silver -> Gold -> Dashboard)

Tools:
  jupyter      Start JupyterLab on port 8888
  test         Run the pytest suite

Any other arguments are executed directly (e.g. bash, spark-submit).
EOF
    ;;

  *)
    exec "$@"
    ;;
esac
