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

  pipeline)
    python -m pipeline.bronze_ingest.bronze_ingest_stream &
    BRONZE_PID=$!
    sleep 5
    python -m pipeline.silver_transform.silver_transform_job &
    SILVER_PID=$!
    sleep 5
    python -m pipeline.gold_aggregations.gold_aggregations_job &
    GOLD_PID=$!
    echo "Pipeline started: bronze=$BRONZE_PID silver=$SILVER_PID gold=$GOLD_PID"
    wait -n
    kill $BRONZE_PID $SILVER_PID $GOLD_PID 2>/dev/null
    ;;

  dashboard)
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

Pipeline layers:
  generator    Run IoT telemetry data generator
  bronze       Run Bronze ingestion stream
  silver       Run Silver transformation stream
  gold         Run Gold aggregation batch job
  pipeline     Run all three layers (Bronze + Silver + Gold) concurrently

Tools:
  dashboard    Generate monitoring dashboard report
  jupyter      Start JupyterLab on port 8888
  test         Run the pytest suite

Any other arguments are executed directly (e.g. bash, spark-submit).
EOF
    ;;

  *)
    exec "$@"
    ;;
esac
