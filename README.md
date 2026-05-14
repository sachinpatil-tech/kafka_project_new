# Users CDC Lakehouse — Containerized Pipeline

End-to-end CDC pipeline:

```
PostgreSQL (mydb.public.users)
   ↓  Debezium logical decoding (pgoutput)
Kafka topic  pgb.public.users
   ↓  PySpark Structured Streaming (or batch)
MinIO  →  Bronze (CDC history) → Silver (current state) → Gold (aggregates)
   ↓  Iceberg + Nessie catalog
Trino  →  Metabase
   ↑  Airflow DAGs orchestrate batch transforms
```

Source rows: `dvdrental.public.actor` (200 rows) → transformed into
`mydb.public.users` (`id`, `name`, `email`). Debezium publishes every change
on topic `pgb.public.users`.

Full architecture, validation commands, MinIO layout, testing steps and
production best practices: see [docs/PIPELINE.md](docs/PIPELINE.md).

---

## Project structure

```
dataLat/
├── app/
│   ├── __init__.py
│   ├── index.py              # CLI entrypoint (--mode kafka|transform|trino)
│   ├── config.py             # env-driven configuration
│   ├── kafka_consumer.py     # Kafka CDC → Bronze (Debezium envelope)
│   ├── transformations.py    # Bronze → Silver → Gold
│   ├── trino_client.py       # Trino analytics
│   └── utils.py
├── airflow/dags/             # Airflow KubernetesPodOperator DAGs
│   ├── users_bronze_load.py
│   ├── users_silver_clean.py
│   ├── users_gold_agg.py
│   └── users_lakehouse_pipeline.py
├── deploy/                   # OpenShift manifests
│   ├── debezium-postgres-connector.yaml
│   ├── bronze-streaming-deployment.yaml
│   ├── bronze-load-job.yaml
│   ├── silver-clean-job.yaml
│   ├── gold-agg-job.yaml
│   ├── trino-query-job.yaml
│   └── secrets-template.yaml
├── sql/
│   ├── 00_source_seed_dvdrental_to_mydb.sql
│   └── 10_trino_users_queries.sql
├── docs/PIPELINE.md
├── Dockerfile
├── entrypoint.sh
├── requirements.txt
└── README.md
```

---

## CLI usage

```bash
# Bronze: stream Debezium CDC into Iceberg Bronze (real-time)
python -m app.index --mode kafka --topic pgb.public.users --consume-mode streaming

# Bronze: one-shot batch backfill
python -m app.index --mode kafka --topic pgb.public.users --consume-mode batch

# Silver: dedup + cleanse Bronze → users_clean
python -m app.index --mode transform --layer silver

# Gold: aggregates (email-domain, daily change tally)
python -m app.index --mode transform --layer gold

# Trino analytics on lakehouse
python -m app.index --mode trino                          # all sections
python -m app.index --mode trino --section silver         # one section
python -m app.index --mode trino --section gold

# Print resolved config (no work)
python -m app.index --mode trino --show-config
```

---

## Build & push to OpenShift internal registry

```bash
# Build via OpenShift BuildConfig (no local Docker needed)
oc new-build --strategy docker --binary --name lakehouse-app -n lakehouse-ingest
oc start-build lakehouse-app --from-dir . -n lakehouse-ingest --follow
```

---

## Deploy the pipeline (in order)

```bash
# 1. Seed source/target Postgres
psql -h $PG_HOST -U postgres -f sql/00_source_seed_dvdrental_to_mydb.sql

# 2. Secrets
oc apply -f deploy/secrets-template.yaml      # edit values first

# 3. Debezium connector → starts publishing to pgb.public.users
oc apply -f deploy/debezium-postgres-connector.yaml

# 4. Either real-time streaming OR scheduled batch
oc apply -f deploy/bronze-streaming-deployment.yaml   # real-time
# OR
oc apply -f deploy/bronze-load-job.yaml               # batch backfill
oc apply -f deploy/silver-clean-job.yaml
oc apply -f deploy/gold-agg-job.yaml

# 5. Airflow DAGs (rsync into scheduler pod)
oc rsync airflow/dags/ $(oc get pod -n airflow -l component=scheduler \
    -o name | head -1):/opt/airflow/dags/ -n airflow

# 6. Smoke-test analytics
oc apply -f deploy/trino-query-job.yaml
oc logs -f job/trino-users-query -n lakehouse-catalog
```

---

## Environment variables

| Variable                  | Default                                          | Purpose                    |
|---------------------------|--------------------------------------------------|----------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `my-cluster-kafka-bootstrap...:9092`             | Kafka endpoint             |
| `KAFKA_TOPIC`             | `pgb.public.users`                               | Debezium CDC topic         |
| `KAFKA_USERNAME`          | `app-user`                                       | SASL username              |
| `KAFKA_PASSWORD`          | (secret)                                         | SASL password              |
| `PG_HOST` / `PG_PORT`     | `postgresql...:5432`                             | Postgres endpoint          |
| `PG_SOURCE_DB`            | `dvdrental`                                      | Source DB                  |
| `PG_TARGET_DB`            | `mydb`                                           | Target DB                  |
| `S3_ENDPOINT`             | `http://minio-api...:9000`                       | MinIO endpoint             |
| `S3_ACCESS_KEY`           | `minioadmin`                                     | MinIO access key           |
| `S3_SECRET_KEY`           | (secret)                                         | MinIO secret key           |
| `NESSIE_URI`              | `http://nessie...:19120/api/v2`                  | Nessie catalog URL         |
| `NESSIE_REF`              | `main`                                           | Nessie branch              |
| `ICEBERG_WAREHOUSE`       | `s3a://lakehouse-warehouse/warehouse`            | Iceberg root path          |
| `TRINO_HOST` / `_PORT`    | `trino...:8080`                                  | Trino coordinator          |
| `TRINO_CATALOG`           | `iceberg`                                        | Default catalog            |
| `TRINO_SCHEMA`            | `bronze`                                         | Default schema             |

---

## Quick validation

```bash
# Kafka — does the topic exist & have data?
oc exec -n lakehouse-ingest my-cluster-kafka-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic pgb.public.users --from-beginning --max-messages 3

# Connector — running?
oc exec -n lakehouse-ingest deploy/debezium-connect-cluster-connect -- \
  curl -s http://localhost:8083/connectors/pgb-users-connector/status | jq

# Trino — row counts across all layers
oc apply -f deploy/trino-query-job.yaml
oc logs -f job/trino-users-query -n lakehouse-catalog
```

Expected after first full run:
- Bronze `users_raw`: **200** (initial snapshots, `op=r`)
- Silver `users_clean`: **200**
- Gold `users_email_domain`: **1** (`example.com` → 200)
- Gold `users_daily_changes`: 1 row for today / `SNAPSHOT`

---

## Troubleshooting

| Symptom                                | Likely cause / fix                                                              |
|----------------------------------------|----------------------------------------------------------------------------------|
| Topic missing                          | Connector not running. Check `/connectors/.../status`                            |
| Snapshot stuck                         | `slot.name` already exists. Drop logical replication slot in PG and rerun.       |
| Bronze empty                           | SASL creds wrong, or `startingOffsets=earliest` not honored — check Spark logs.  |
| Silver 0 rows but Bronze has rows      | All bronze events had null `email`. Inspect with `SELECT * FROM bronze.users_raw LIMIT 5` |
| Iceberg `NoSuchTableException`         | `lakehouse.bronze` namespace not created — re-run consumer (it ensures it).      |
| Trino: "iceberg catalog not found"     | `iceberg.properties` not mounted in Trino configmap.                             |

---

## License

Proprietary — internal use only.
