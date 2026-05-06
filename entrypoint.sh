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

if [ "$MODE" = "kafka" ]; then
    echo "==> Running kafka mode via spark-submit"
    exec /opt/spark/bin/spark-submit \
        --conf spark.driver.extraJavaOptions=-Daws.region=${AWS_REGION} \
        --conf spark.executor.extraJavaOptions=-Daws.region=${AWS_REGION} \
        -m app.index "$@"

elif [ "$MODE" = "trino" ]; then
    echo "==> Running trino mode"
    exec python3 -m app.index "$@"

else
    echo "==> Default: running python3"
    exec python3 -m app.index "$@"
fi