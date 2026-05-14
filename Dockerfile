# ================================================================
# Dockerfile — Banking Lakehouse Application (FIXED)
# ================================================================

FROM apache/spark:3.5.4-python3

USER root

LABEL maintainer="Abdullah Shaikh"
LABEL description="Banking Lakehouse containerized app"
LABEL version="1.0"

# ---- Environment defaults ----
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AWS_REGION=us-east-1 \
    KAFKA_BOOTSTRAP_SERVERS=my-cluster-kafka-bootstrap.lakehouse-ingest.svc.cluster.local:9092 \
    KAFKA_TOPIC=pgb.public.users \
    KAFKA_USERNAME=app-user \
    S3_ENDPOINT=http://minio-api.lakehouse-data.svc.cluster.local:9000 \
    S3_ACCESS_KEY=minioadmin \
    ICEBERG_WAREHOUSE=s3a://lakehouse-warehouse/warehouse \
    NESSIE_URI=http://nessie.lakehouse-catalog.svc:19120/api/v2 \
    TRINO_HOST=trino-external.lakehouse-catalog.svc.cluster.local \
    TRINO_PORT=8080 \
    TRINO_USER=admin \
    TRINO_CATALOG=iceberg \
    TRINO_SCHEMA=bronze \
    TRINO_HTTP_SCHEME=http

# ---- Download Iceberg / Nessie / Kafka jars at build time ----
WORKDIR /opt/spark/jars

RUN set -eux; \
    for jar in \
        "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar" \
        "org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar" \
        "org/apache/iceberg/iceberg-nessie/1.7.1/iceberg-nessie-1.7.1.jar" \
        "org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.92.1/nessie-spark-extensions-3.5_2.12-0.92.1.jar" \
        "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.4/spark-sql-kafka-0-10_2.12-3.5.4.jar" \
        "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.4/spark-token-provider-kafka-0-10_2.12-3.5.4.jar" \
        "org/apache/kafka/kafka-clients/3.6.1/kafka-clients-3.6.1.jar" \
        "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
    ; do \
        filename=$(basename "$jar"); \
        curl -fsSL -o "$filename" "https://repo1.maven.org/maven2/$jar"; \
    done

# ---- Install Python dependencies ----
WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# ---- Copy application code ----
COPY app/ /app/app/
COPY main.py /app/main.py

# ---- Copy entrypoint ----
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# ---- Fix permissions and line endings ----
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh && \
    chown 185:185 /usr/local/bin/entrypoint.sh && \
    chown -R 185:185 /app

# ---- Run as Spark non-root user ----
USER 185

# ---- Entrypoint ----
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# ---- Default command ----
CMD ["--mode", "all"]