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
export PYTHONPATH=/app:${PYTHONPATH}

case "$MODE" in
    kafka|transform|all|streaming)
        echo "==> Running ${MODE} mode via spark-submit"
        # Streaming-specific spark tuning: smaller exec, longer task timeout
        EXTRA_CONF=""
        if [ "$MODE" = "streaming" ]; then
            EXTRA_CONF="--conf spark.streaming.stopGracefullyOnShutdown=true \
                        --conf spark.sql.streaming.gracefulShutdown.enabled=true \
                        --conf spark.driver.memory=2g \
                        --conf spark.executor.memory=2g"
        fi
        exec /opt/spark/bin/spark-submit \
            --conf spark.driver.extraJavaOptions=-Daws.region=${AWS_REGION} \
            --conf spark.executor.extraJavaOptions=-Daws.region=${AWS_REGION} \
            ${EXTRA_CONF} \
            /app/main.py "$@"
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
