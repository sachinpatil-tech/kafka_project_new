#!/bin/bash
set -e

# Parse --mode flag to decide execution strategy
MODE=""
prev=""
for arg in "$@"; do
    if [ "$prev" = "--mode" ]; then
        MODE="$arg"
        break
    fi
    prev="$arg"
done

cd /app

case "$MODE" in
    kafka|transform|all)
        echo "==> Running ${MODE} mode via spark-submit"
        exec /opt/spark/bin/spark-submit \
            --conf spark.driver.extraJavaOptions=-Daws.region=${AWS_REGION} \
            --conf spark.executor.extraJavaOptions=-Daws.region=${AWS_REGION} \
            -m app.index "$@"
        ;;
    trino|probe)
        echo "==> Running ${MODE} mode"
        exec python3 -m app.index "$@"
        ;;
    *)
        echo "==> Default: running python3"
        exec python3 -m app.index "$@"
        ;;
esac
