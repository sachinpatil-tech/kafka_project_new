# Users CDC Lakehouse — End-to-End Pipeline

Source: `dvdrental.actor` → transformed into `mydb.public.users` (200 rows).
Debezium publishes CDC events to Kafka topic `pgb.public.users`.
Spark consumes, lands Bronze/Silver/Gold in MinIO via Nessie catalog.
Trino + Metabase serve analytics.

---

## 1. Architecture flow

```
 ┌──────────────────────┐
 │ PostgreSQL: dvdrental│  (source — actor table, 200 rows)
 │   public.actor       │
 └──────────┬───────────┘
            │  SQL INSERT (id, name, email)
            ▼
 ┌──────────────────────┐
 │ PostgreSQL: mydb     │  (target — users table)
 │   public.users       │  ◄── REPLICA IDENTITY FULL
 └──────────┬───────────┘
            │  WAL (logical decoding via pgoutput)
            ▼
 ┌──────────────────────┐
 │ Debezium Connector   │  (pgb-users-connector)
 │   PostgresConnector  │
 └──────────┬───────────┘
            │  JSON envelope { before, after, op, ts_ms, source }
            ▼
 ┌──────────────────────┐
 │ Kafka                │  topic: pgb.public.users
 │  (Strimzi cluster)   │  (3 partitions, 7d retention)
 └──────────┬───────────┘
            │  Spark Structured Streaming (or batch backfill)
            ▼
 ┌──────────────────────┐
 │ Spark (PySpark)      │  Bronze ingestion job
 │  lakehouse-app:1.1   │
 └──────────┬───────────┘
            │  Iceberg append (Nessie-catalog'd)
            ▼
 ┌──────────────────────┐
 │ MinIO                │  s3a://lakehouse-warehouse/warehouse/
 │  (object storage)    │   bronze.db/users_raw/
 │                      │   silver.db/users_clean/
 │                      │   gold.db/users_email_domain/
 │                      │   gold.db/users_daily_changes/
 └──────────┬───────────┘
            │  Iceberg metadata in Nessie (versioned commits)
            ▼
 ┌──────────────────────┐
 │ Nessie catalog       │  branch: main
 └──────────┬───────────┘
            │
            ▼
 ┌──────────────────────┐
 │ Trino                │  catalog: iceberg
 │  (query engine)      │
 └──────────┬───────────┘
            │  JDBC
            ▼
 ┌──────────────────────┐
 │ Metabase             │  dashboards
 └──────────────────────┘

 Orchestration: Airflow DAGs (KubernetesPodOperator)
   - users_bronze_load      (manual / backfill)
   - users_silver_clean     (@hourly)
   - users_gold_agg         (@daily)
   - users_lakehouse_pipeline (full chain, @hourly)

 Real-time path: Deployment `bronze-users-stream`
   (Spark Structured Streaming, 30-second triggers)
```

---

## 2. Data mapping

| Source (`dvdrental.public.actor`) | Target (`mydb.public.users`)        |
|-----------------------------------|--------------------------------------|
| `actor_id`                        | `id`                                 |
| `first_name + ' ' + last_name`    | `name`                               |
| generated `firstname.lastname@example.com` | `email`                     |

Result: **200 records** in `mydb.public.users`.

---

## 3. Lakehouse table layout (MinIO)

```
s3://lakehouse-warehouse/warehouse/
├── bronze.db/
│   └── users_raw/                         (Iceberg)
│       ├── metadata/  *.metadata.json, *.avro
│       └── data/      *.parquet
├── silver.db/
│   └── users_clean/                       (Iceberg)
│       ├── metadata/
│       └── data/
└── gold.db/
    ├── users_email_domain/                (Iceberg)
    └── users_daily_changes/               (Iceberg)

s3://lakehouse-checkpoints/users/         (Spark streaming checkpoints)
```

---

## 4. Deploy commands (one-shot)

```bash
# A. Seed the source/target (run in a postgres-client pod)
oc rsh -n lakehouse-ingest postgresql-0 \
  psql -h localhost -U postgres -d postgres \
       -f /tmp/00_source_seed_dvdrental_to_mydb.sql

# B. Apply secrets (edit values first!)
oc apply -f deploy/secrets-template.yaml

# C. Register Debezium connector
oc apply -f deploy/debezium-postgres-connector.yaml

# D. Build the app image (BuildConfig)
oc new-build --strategy docker --binary --name lakehouse-app -n lakehouse-ingest || true
oc start-build lakehouse-app --from-dir . -n lakehouse-ingest --follow

# E. Either run streaming OR Airflow-orchestrated batch
oc apply -f deploy/bronze-streaming-deployment.yaml    # real-time
# ...or trigger one-shot jobs:
oc apply -f deploy/bronze-load-job.yaml
oc apply -f deploy/silver-clean-job.yaml
oc apply -f deploy/gold-agg-job.yaml
oc apply -f deploy/trino-query-job.yaml

# F. Deploy Airflow DAGs (rsync to scheduler's dags folder)
oc rsync airflow/dags/ \
   $(oc get pod -n airflow -l component=scheduler -o name | head -1):/opt/airflow/dags/ \
   -n airflow
```

---

## 5. Validation / proof

### 5.1 Kafka topic + messages

```bash
# Confirm topic exists
oc exec -n lakehouse-ingest my-cluster-kafka-0 -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep pgb.public.users

# Peek messages (Debezium envelope JSON)
oc exec -n lakehouse-ingest my-cluster-kafka-0 -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic pgb.public.users --from-beginning --max-messages 3
```

Expected: 200 snapshot events (`"op":"r"`) for the initial load, then `c/u/d`
on subsequent changes.

### 5.2 Connector health

```bash
oc exec -n lakehouse-ingest deploy/debezium-connect-cluster-connect -- \
  curl -s http://localhost:8083/connectors/pgb-users-connector/status | jq
```

Expected: `"connector": { "state": "RUNNING" }`, `"tasks": [ { "state": "RUNNING" } ]`.

### 5.3 Bronze written

```bash
# Quick count via the trino smoke-test job
oc logs -n lakehouse-catalog job/trino-users-query | grep users_raw
```

### 5.4 MinIO files

```bash
oc exec -n lakehouse-data deploy/minio -- \
  mc ls -r local/lakehouse-warehouse/warehouse/bronze.db/users_raw/data/ | head
```

### 5.5 Trino interactive

```bash
oc exec -it -n lakehouse-catalog deploy/trino -- trino \
  --catalog iceberg --schema silver --execute "SELECT COUNT(*) FROM users_clean"
```
Expected on first full run: **200**.

---

## 6. Expected outputs

| Layer  | Table                       | Approx rows (first run)        |
|--------|-----------------------------|---------------------------------|
| Bronze | `users_raw`                 | 200 (all `op=r` snapshots)      |
| Silver | `users_clean`               | 200 (deduped per id)            |
| Gold   | `users_email_domain`        | 1 (`example.com` → 200)         |
| Gold   | `users_daily_changes`       | 1 row per (date, op_type)       |

After an UPDATE on a user:

| Layer  | Effect                                              |
|--------|------------------------------------------------------|
| Bronze | +1 row (`op=u`, new before/after image)              |
| Silver | row replaced (same id, latest values win)            |
| Gold   | unchanged unless domain changed                      |

---

## 7. Testing steps

1. **Initial snapshot** — apply connector, watch 200 `op=r` events appear.
2. **Bronze batch consume** — `oc apply -f deploy/bronze-load-job.yaml`,
   then `oc logs -f job/bronze-users-batch` → expect "200 CDC events".
3. **Silver/Gold** — run silver-clean + gold-agg jobs, verify Trino counts.
4. **CDC update test**:
   ```sql
   -- in mydb
   UPDATE public.users SET name = 'PENELOPE GUINESS-UPDATED' WHERE id = 1;
   ```
   Within 30s, Bronze should grow by 1; rerun silver-clean and Silver row #1
   should reflect new name.
5. **CDC delete test**:
   ```sql
   DELETE FROM public.users WHERE id = 200;
   ```
   Bronze grows by 1 (`op=d`); after silver rebuild, row 200 is gone from
   `users_clean`.
6. **Metabase** — connect to Trino, build pie chart from
   `iceberg.gold.users_email_domain`, line chart from
   `iceberg.gold.users_daily_changes`.

---

## 8. Production best practices

- **Idempotent transforms** — Silver/Gold use `createOrReplace`; safe to
  retry without dedup logic in downstream.
- **Append-only Bronze** — preserves full CDC history; never `OVERWRITE`.
- **Checkpoints in object storage** — `s3a://lakehouse-checkpoints/users`
  so pod restart resumes from last offset.
- **Snapshot Iceberg metadata** — expire snapshots after 7d, remove orphan
  files weekly:
  ```sql
  CALL iceberg.system.expire_snapshots('bronze.users_raw', TIMESTAMP '...');
  CALL iceberg.system.remove_orphan_files('bronze.users_raw');
  ```
- **Schema evolution** — Iceberg + Debezium handle add-column safely; for
  type changes, branch in Nessie before applying.
- **Secrets** — never bake passwords into ENV in the image; use OpenShift
  Secrets mounted via `envFrom`/`secretKeyRef`.
- **Resource governance** — set requests/limits; isolate streaming pods
  via `nodeSelector` if cluster is shared.
- **Replica identity FULL** — required on source tables for clean `before`
  images in updates/deletes.

---

## 9. Monitoring

| Component        | Check                                                              |
|------------------|--------------------------------------------------------------------|
| Debezium         | `/connectors/pgb-users-connector/status` (REST) — must be RUNNING  |
| Kafka lag        | `kafka-consumer-groups.sh --describe` for the Spark consumer group |
| Streaming Spark  | pod `restartCount`, `kubectl top pod`, structured-streaming UI     |
| Airflow          | DAG run state, SLA miss alerts, task duration                      |
| MinIO            | bucket size growth, object count                                   |
| Trino            | query latency p95, failed queries                                  |
| Metabase         | dashboard load time                                                |

Recommended Prometheus rules: alert if connector `state != RUNNING`,
streaming `processedRowsPerSecond == 0` for >10 min, or Bronze row count
hasn't increased in >1h despite source changes.

---

## 10. Folder layout

```
dataLat/
├── app/                              # Python package (Spark + Trino)
│   ├── __init__.py
│   ├── config.py                     # env-driven config
│   ├── index.py                      # CLI dispatch
│   ├── kafka_consumer.py             # CDC → Bronze
│   ├── transformations.py            # Bronze → Silver → Gold
│   ├── trino_client.py               # analytics queries
│   └── utils.py
├── airflow/dags/                     # Airflow DAGs
│   ├── users_bronze_load.py
│   ├── users_silver_clean.py
│   ├── users_gold_agg.py
│   └── users_lakehouse_pipeline.py
├── deploy/                           # OpenShift manifests
│   ├── debezium-postgres-connector.yaml
│   ├── bronze-streaming-deployment.yaml
│   ├── bronze-load-job.yaml
│   ├── silver-clean-job.yaml
│   ├── gold-agg-job.yaml
│   ├── trino-query-job.yaml
│   └── secrets-template.yaml
├── sql/                              # source seed + Trino queries
│   ├── 00_source_seed_dvdrental_to_mydb.sql
│   └── 10_trino_users_queries.sql
├── docs/
│   └── PIPELINE.md                   # this file
├── Dockerfile
├── entrypoint.sh
├── requirements.txt
└── README.md
```
