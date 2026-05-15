# Spark Structured Streaming — Users CDC Lakehouse

Continuously consume Debezium CDC events from Kafka and update
Bronze / Silver / Gold Iceberg tables in near real-time.

---

## 1. Architecture

```
 ┌─────────────────────────────┐
 │ Kafka topic                 │
 │   pgb.public.users          │  (Debezium-managed, JSON envelope)
 └──────────────┬──────────────┘
                │ readStream (PLAINTEXT, earliest → tail)
                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ Spark Structured Streaming app (lakehouse-app:streaming)    │
 │                                                              │
 │  parse Debezium envelope (before/after/op/ts_ms/source)     │
 │            │                                                 │
 │            ▼                                                 │
 │  foreachBatch(batch_df, batch_id):                           │
 │     ┌──────────────────────────────────────────────────┐    │
 │     │  STAGE 1 — BRONZE                                 │    │
 │     │  batch_df.writeTo("lakehouse.bronze.users_raw")   │    │
 │     │           .append()                                │    │
 │     │  → immutable CDC history                          │    │
 │     └──────────────────────────────────────────────────┘    │
 │     ┌──────────────────────────────────────────────────┐    │
 │     │  STAGE 2 — SILVER                                 │    │
 │     │  MERGE INTO lakehouse.silver.users_clean          │    │
 │     │   USING (latest row per id from batch) src        │    │
 │     │   ON t.id = src.id                                │    │
 │     │   WHEN MATCHED AND DELETE → DELETE                │    │
 │     │   WHEN MATCHED AND ts >= old → UPDATE             │    │
 │     │   WHEN NOT MATCHED AND not DELETE → INSERT        │    │
 │     │  → idempotent upsert, latest state per id         │    │
 │     └──────────────────────────────────────────────────┘    │
 │     ┌──────────────────────────────────────────────────┐    │
 │     │  STAGE 3 — GOLD                                   │    │
 │     │  INSERT OVERWRITE gold.users_email_domain ...     │    │
 │     │  INSERT OVERWRITE gold.users_daily_changes ...    │    │
 │     │  → atomic snapshot rebuild from Silver/Bronze     │    │
 │     └──────────────────────────────────────────────────┘    │
 │                                                              │
 │  checkpoint: s3a://lakehouse-warehouse/checkpoints/...      │
 │  trigger:    processingTime="30 seconds"                    │
 │  awaitTermination() → never exits                           │
 └─────────────────────────────────────────────────────────────┘
                │
                ▼ (Iceberg + Nessie commits)
 ┌─────────────────────────────┐
 │ MinIO (parquet + metadata)  │
 └──────────────┬──────────────┘
                ▼
 ┌─────────────────────────────┐
 │ Trino / Metabase            │  ← always sees latest snapshot
 └─────────────────────────────┘
```

---

## 2. Why `foreachBatch` instead of native `writeStream.format("iceberg")`?

| Need | Native streaming write | `foreachBatch` (chosen) |
|------|------------------------|--------------------------|
| Bronze append | ✓ | ✓ |
| Silver MERGE (upsert + delete) | ✗ — only append | ✓ — full DML |
| Gold INSERT OVERWRITE per batch | ✗ | ✓ |
| All 3 layers in one streaming query | ✗ — needs 3 queries | ✓ — one checkpoint, one driver |

So we have **one** streaming source (Kafka), **one** checkpoint, and three
table updates handled atomically per micro-batch.

---

## 3. Idempotency & exactly-once

Spark Structured Streaming gives **at-least-once** delivery by default; we
upgrade this to **effectively-exactly-once** via:

1. **Kafka offsets in checkpoint** — on restart, Spark replays only
   un-committed offsets.
2. **MERGE INTO Silver** — re-applying the same CDC event is a no-op
   (`ts >= old_ts` short-circuits update if already applied).
3. **INSERT OVERWRITE Gold** — always rebuilt from Silver, no duplicates.
4. **Bronze append** — only Bronze can have duplicates on retry; mitigated
   because the checkpoint commits BEFORE the next batch starts. If a batch
   fails after Bronze write but before checkpoint, the same Bronze rows
   will be appended again on retry — acceptable, since Bronze is "events as
   history". Use `kafka_offset` to dedupe downstream if strict-exact needed.

---

## 4. Checkpoint strategy

```
s3a://lakehouse-warehouse/checkpoints/users-streaming/
├── offsets/         ← Kafka offset commits (per micro-batch)
├── commits/         ← marker files (committed batches)
├── sources/0/       ← source metadata
└── state/           ← (n/a — we have no stateful agg)
```

**Rules:**
- Checkpoint MUST be on durable object storage (NOT pod-local fs).
- Each streaming app must have its own checkpoint path.
- Deleting the checkpoint = reset to beginning (full replay).
- Code changes that alter the query plan may require checkpoint reset.

---

## 5. Failure handling

| Failure | What happens | Recovery |
|---------|---------------|----------|
| Pod crash (OOM, SIGKILL) | Deployment respawns pod | Resume from checkpoint, no data loss |
| Kafka unavailable | Streaming task fails | Spark retries (`retries=Integer.MAX_VALUE`), backoff |
| MinIO/Nessie unavailable | Batch fails | Spark retries same micro-batch; checkpoint NOT advanced |
| Schema drift in source | Parsing returns nulls | Schema is permissive — null columns persisted to Bronze |
| Bad JSON message | `from_json` returns null payload | Filter `WHERE id IS NOT NULL` skips it |
| Long batch (slow MERGE) | Lag grows on Kafka | Increase trigger interval OR `maxOffsetsPerTrigger` |

---

## 6. Watermarking — do we need it?

**Not in this pipeline.** Watermarking is needed when you do:
- Event-time windowed aggregations
- Stream-stream joins
- Late event eviction

We do **stateless** processing per batch (MERGE + INSERT OVERWRITE),
so no watermark is required. If you later add windowed metrics like
"updates per 5-minute tumbling window", add:

```python
df_parsed.withWatermark("source_ts", "10 minutes")
         .groupBy(window("source_ts", "5 minutes"), "op_type")
         .count()
```

---

## 7. Iceberg streaming write recommendations

Every micro-batch creates a **new snapshot** in Iceberg. Over a day at 30s
trigger you get ~2880 snapshots. Manage this:

### 7.1 Snapshot expiration (daily)
Add an Airflow DAG that runs:
```sql
CALL lakehouse.system.expire_snapshots(
  table => 'bronze.users_raw',
  older_than => TIMESTAMP '2026-05-13 00:00:00',
  retain_last => 100
);
```

### 7.2 Small-file compaction (weekly)
```sql
CALL lakehouse.system.rewrite_data_files(
  table => 'bronze.users_raw',
  options => map('min-input-files', '10')
);
```

### 7.3 Orphan file removal (weekly)
```sql
CALL lakehouse.system.remove_orphan_files(
  table => 'bronze.users_raw'
);
```

### 7.4 Metadata cleanup
Already enabled via TBLPROPERTIES:
```
write.metadata.delete-after-commit.enabled = true
write.metadata.previous-versions-max       = 20
```

---

## 8. Deployment (OpenShift)

Use **Deployment**, not Job:

```bash
oc apply -f deploy/bronze-streaming-deployment.yaml
```

Verify:
```bash
oc get pods -n lakehouse-ingest -l component=streaming
oc logs -f deployment/users-cdc-streaming -n lakehouse-ingest
```

Look for these log lines:
```
STREAMING ACTIVE — never exits.
Batch 0: 1508 new CDC events received
Batch 0: ✓ Bronze appended 1508 rows
Batch 0: ✓ Silver MERGE done
Batch 0: ✓ Gold rebuilt
Progress: batch=1, input_rows=0, processed_rows/sec=0.0, ...
```

### Force redeploy after code change
```bash
oc rollout restart deployment/users-cdc-streaming -n lakehouse-ingest
```

---

## 9. Tunable env vars

| ENV                          | Default                                                    | Purpose |
|------------------------------|------------------------------------------------------------|---------|
| `STREAMING_TRIGGER_SECONDS`  | `30`                                                       | Micro-batch interval |
| `STREAMING_CHECKPOINT`       | `s3a://lakehouse-warehouse/checkpoints/users-streaming`   | Checkpoint location |
| `STREAMING_QUERY_NAME`       | `users-cdc-streaming`                                      | Query name for metrics |
| `KAFKA_TOPIC`                | `pgb.public.users`                                         | Topic to consume |
| `KAFKA_SECURITY_PROTOCOL`    | `PLAINTEXT`                                                | Match broker listener |

Lower trigger to **5 seconds** for sub-second-feel dashboards; raise to
**5 minutes** for low-cost batch-y workloads.

---

## 10. Monitoring

Streaming progress is logged every 60 seconds:
```
Progress: batch=N, input_rows=N, processed_rows/sec=X, duration_ms=Y
```

For Prometheus/Grafana:
- `spark.sql.streaming.metricsEnabled = true` is already set
- Scrape Spark driver metrics endpoint on port 4040
- Key metrics: `input_rate`, `processing_rate`, `latency`,
  `batches_per_minute`

### Alerts
- **Processing rate = 0 for >5 min** while Kafka has lag → pipeline stuck
- **Latency > trigger × 3** → falling behind, scale up
- **Pod `restartCount > 0`** → investigate OOM / network issues

---

## 11. Switching between batch and streaming

| When | Command |
|------|---------|
| Initial backfill (one-shot) | `--mode all` |
| Continuous streaming | `--mode streaming` (default) |
| Reset streaming (rare) | Delete checkpoint dir, redeploy |

Default `CMD` in Dockerfile = `["--mode", "streaming"]`, so Tekton's Job
template will run the streaming app. **Note:** the Tekton Job will show
status "Running" forever; this is expected for streaming workloads. For
production, use the `Deployment` in `deploy/` instead.

---

## 12. Metabase auto-refresh

Trino sees the new Iceberg snapshot immediately after each `INSERT
OVERWRITE` / `MERGE`. Metabase dashboards:

- Set dashboard refresh interval to **1 minute** (or below trigger seconds)
- All queries against `iceberg.silver.users_clean` and
  `iceberg.gold.*` return latest snapshot automatically

No special Metabase config needed — Iceberg snapshots are transparent.
