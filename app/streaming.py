"""
================================================================
 streaming.py — Long-running Structured Streaming pipeline
================================================================
 Kafka (Debezium CDC topic 'pgb.public.users') ko continuously
 padhke ek hi streaming query me Bronze→Silver→Gold update karta hai.

 Architecture:
   readStream(Kafka)
       ↓ parse Debezium envelope
       ↓ foreachBatch(batch_df, batch_id)
            ├── Bronze   : APPEND raw CDC events           (immutable history)
            ├── Silver   : MERGE INTO (upsert + delete)    (latest state per id)
            └── Gold     : INSERT OVERWRITE aggregates     (rebuilt per batch)
       ↓ checkpoint to s3a://.../checkpoints/users-streaming
       ↓ awaitTermination() — never exits

 Idempotency: Kafka offsets in checkpoint + MERGE INTO ensures
 re-running a batch is safe (no duplicates in Silver/Gold).
================================================================
"""
import os
import time
from datetime import datetime

from app import config
from app.utils import get_logger, print_section

log = get_logger("streaming")


# ----------------------------------------------------------------
# CONSTANTS
# ----------------------------------------------------------------
BRONZE_USERS = "lakehouse.bronze.users_raw"
SILVER_USERS = "lakehouse.silver.users_clean"
GOLD_DOMAIN  = "lakehouse.gold.users_email_domain"
GOLD_CHANGES = "lakehouse.gold.users_daily_changes"

# Checkpoint must live on durable storage (object store), not pod fs.
CHECKPOINT = os.environ.get(
    "STREAMING_CHECKPOINT",
    "s3a://lakehouse-warehouse/checkpoints/users-streaming"
)
TRIGGER_SECONDS = int(os.environ.get("STREAMING_TRIGGER_SECONDS", "30"))
QUERY_NAME      = os.environ.get("STREAMING_QUERY_NAME", "users-cdc-streaming")


# ----------------------------------------------------------------
# SPARK SESSION
# ----------------------------------------------------------------
def _create_spark_session():
    """Spark session with Iceberg + Nessie + MinIO wiring + streaming tuning."""
    from pyspark.sql import SparkSession

    log.info("Creating Spark session for streaming")
    spark = (
        SparkSession.builder
        .appName("LakehouseUsersStreaming")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Iceberg catalog backed by Nessie
        .config("spark.sql.catalog.lakehouse",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl",
                "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.lakehouse.uri",       config.NESSIE_URI)
        .config("spark.sql.catalog.lakehouse.ref",       config.NESSIE_REF)
        .config("spark.sql.catalog.lakehouse.warehouse", config.ICEBERG_WAREHOUSE)
        .config("spark.sql.catalog.lakehouse.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint",          config.S3_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id",     config.S3_ACCESS_KEY)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", config.S3_SECRET_KEY)
        .config("spark.sql.catalog.lakehouse.s3.region",            config.S3_REGION)
        # Hadoop S3A — used by streaming checkpoint location
        .config("spark.hadoop.fs.s3a.endpoint",          config.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        config.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        config.S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # ★ Force FileSystem-based checkpoint manager — avoids the missing
        #   org.apache.hadoop.fs.s3a.S3A (AbstractFileSystem) class that
        #   FileContext API would otherwise need.
        .config("spark.sql.streaming.checkpointFileManagerClass",
                "org.apache.spark.sql.execution.streaming."
                "FileSystemBasedCheckpointFileManager")
        # AbstractFileSystem mapping (also provided in case FileContext is used
        # elsewhere). Both jars ship S3A; this just makes the mapping explicit.
        .config("spark.hadoop.fs.AbstractFileSystem.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3A")
        .config("spark.sql.defaultCatalog", "lakehouse")
        # Streaming-friendly settings
        .config("spark.sql.shuffle.partitions", "4")        # small workload
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.sql.streaming.minBatchesToRetain", "20")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")  # quieter logs in streaming
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    log.info("Spark session ready")
    return spark


# ----------------------------------------------------------------
# TABLE BOOTSTRAP — pre-create with full schema so MERGE / OVERWRITE works
# ----------------------------------------------------------------
def _bootstrap_tables(spark):
    """Idempotent CREATE TABLE IF NOT EXISTS for Bronze / Silver / Gold."""
    log.info("Bootstrapping Iceberg tables (CREATE IF NOT EXISTS)")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_USERS} (
            kafka_key        STRING,
            kafka_topic      STRING,
            kafka_partition  INT,
            kafka_offset     BIGINT,
            kafka_timestamp  TIMESTAMP,
            op               STRING,
            source_ts_ms     BIGINT,
            source_db        STRING,
            source_schema    STRING,
            source_table     STRING,
            source_lsn       BIGINT,
            before_id        INT,
            before_name      STRING,
            before_email     STRING,
            after_id         INT,
            after_name       STRING,
            after_email      STRING,
            id               INT,
            op_type          STRING,
            _ingestion_ts    TIMESTAMP,
            _source          STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.parquet.compression-codec' = 'zstd',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '20'
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_USERS} (
            id                  INT,
            name                STRING,
            email               STRING,
            email_domain        STRING,
            source_ts_ms        BIGINT,
            source_ts           TIMESTAMP,
            silver_updated_at   TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.merge.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.delete.mode' = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DOMAIN} (
            email_domain   STRING,
            user_count     BIGINT,
            gold_built_at  TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_CHANGES} (
            change_date    DATE,
            op_type        STRING,
            event_count    BIGINT,
            gold_built_at  TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """)
    log.info("✓ All tables ready")


# ----------------------------------------------------------------
# PER-MICRO-BATCH PROCESSOR
# ----------------------------------------------------------------
def _process_micro_batch(batch_df, batch_id):
    """
    Called by Structured Streaming for each micro-batch.
    Order: Bronze append → Silver MERGE → Gold rebuild.
    Re-running same batch is safe (idempotent).
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    count = batch_df.count()
    if count == 0:
        log.info("Batch %s: no new CDC events", batch_id)
        return

    log.info("Batch %s: %d new CDC events received", batch_id, count)
    batch_df.cache()

    try:
        # ---------- STAGE 1: Bronze APPEND ----------
        t0 = time.time()
        batch_df.writeTo(BRONZE_USERS).append()
        log.info("Batch %s: ✓ Bronze appended %d rows (%.2fs)",
                 batch_id, count, time.time() - t0)

        # ---------- STAGE 2: Silver MERGE ----------
        t0 = time.time()
        batch_df.createOrReplaceTempView("micro_batch")
        spark.sql(f"""
            MERGE INTO {SILVER_USERS} t
            USING (
                SELECT * FROM (
                    SELECT
                        id,
                        TRIM(COALESCE(after_name, before_name))         AS name,
                        LOWER(TRIM(COALESCE(after_email, before_email))) AS email,
                        CASE
                            WHEN COALESCE(after_email, before_email) LIKE '%@%'
                            THEN LOWER(SPLIT(TRIM(COALESCE(after_email, before_email)), '@')[1])
                            ELSE NULL
                        END                                              AS email_domain,
                        source_ts_ms,
                        CAST(source_ts_ms / 1000 AS TIMESTAMP)           AS source_ts,
                        op_type,
                        ROW_NUMBER() OVER (
                            PARTITION BY id
                            ORDER BY source_ts_ms DESC,
                                     source_lsn   DESC NULLS LAST,
                                     kafka_offset DESC
                        ) AS rn
                    FROM micro_batch
                    WHERE id IS NOT NULL
                ) WHERE rn = 1
            ) s
            ON t.id = s.id
            WHEN MATCHED AND s.op_type = 'DELETE'
                THEN DELETE
            WHEN MATCHED AND s.source_ts_ms >= COALESCE(t.source_ts_ms, 0)
                THEN UPDATE SET
                    name              = s.name,
                    email             = s.email,
                    email_domain      = s.email_domain,
                    source_ts_ms      = s.source_ts_ms,
                    source_ts         = s.source_ts,
                    silver_updated_at = current_timestamp()
            WHEN NOT MATCHED AND s.op_type <> 'DELETE'
                THEN INSERT (id, name, email, email_domain,
                             source_ts_ms, source_ts, silver_updated_at)
                VALUES (s.id, s.name, s.email, s.email_domain,
                        s.source_ts_ms, s.source_ts, current_timestamp())
        """)
        log.info("Batch %s: ✓ Silver MERGE done (%.2fs)",
                 batch_id, time.time() - t0)

        # ---------- STAGE 3: Gold rebuild (INSERT OVERWRITE — atomic) ----------
        t0 = time.time()
        spark.sql(f"""
            INSERT OVERWRITE {GOLD_DOMAIN}
            SELECT
                email_domain,
                COUNT(*)               AS user_count,
                current_timestamp()    AS gold_built_at
            FROM {SILVER_USERS}
            WHERE email_domain IS NOT NULL
            GROUP BY email_domain
        """)

        spark.sql(f"""
            INSERT OVERWRITE {GOLD_CHANGES}
            SELECT
                DATE(CAST(source_ts_ms / 1000 AS TIMESTAMP)) AS change_date,
                op_type,
                COUNT(*)                                     AS event_count,
                current_timestamp()                          AS gold_built_at
            FROM {BRONZE_USERS}
            WHERE source_ts_ms IS NOT NULL
            GROUP BY DATE(CAST(source_ts_ms / 1000 AS TIMESTAMP)), op_type
        """)
        log.info("Batch %s: ✓ Gold rebuilt (%.2fs)",
                 batch_id, time.time() - t0)

    except Exception as e:
        log.exception("Batch %s FAILED: %s", batch_id, e)
        # Re-raise so Spark fails the batch → retry uses checkpoint
        raise
    finally:
        batch_df.unpersist()


# ----------------------------------------------------------------
# PUBLIC ENTRYPOINT
# ----------------------------------------------------------------
def run() -> int:
    """Long-running streaming entrypoint. Returns 0 on graceful stop."""
    from app.kafka_consumer import (_build_schemas, _get_kafka_options,
                                    _parse_cdc)

    print_section("STREAMING APP — users CDC (continuous)")
    log.info("Started at:           %s", datetime.now().isoformat())
    log.info("Kafka topic:          %s", config.KAFKA_TOPIC)
    log.info("Checkpoint location:  %s", CHECKPOINT)
    log.info("Trigger interval:     every %d seconds", TRIGGER_SECONDS)
    log.info("Query name:           %s", QUERY_NAME)

    spark = _create_spark_session()
    _bootstrap_tables(spark)

    schemas = _build_schemas()
    topic = config.KAFKA_TOPIC
    if topic not in schemas:
        log.error("Unknown topic: %s (available: %s)", topic, list(schemas))
        return 1
    schema, _ = schemas[topic]
    kafka_opts = _get_kafka_options()

    log.info("Subscribing to Kafka topic '%s'", topic)
    df_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_opts)
        .option("subscribe", topic)
        .option("maxOffsetsPerTrigger", "10000")   # backpressure
        .load()
    )

    df_parsed = _parse_cdc(df_stream, schema)

    log.info("Starting streaming query...")
    query = (
        df_parsed.writeStream
        .foreachBatch(_process_micro_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .queryName(QUERY_NAME)
        .start()
    )

    log.info("=" * 72)
    log.info("STREAMING ACTIVE — never exits. Press Ctrl+C to stop.")
    log.info("  Query ID:        %s", query.id)
    log.info("  Run ID:          %s", query.runId)
    log.info("  Spark UI:        http://localhost:4040")
    log.info("=" * 72)

    # Progress logger runs in a daemon thread so it can't block shutdown.
    import threading

    def _progress_logger():
        while query.isActive:
            time.sleep(60)
            lp = query.lastProgress
            if lp:
                log.info(
                    "Progress: batch=%s, input_rows=%s, processed_rows/sec=%.1f, "
                    "duration_ms=%s",
                    lp.get("batchId"),
                    lp.get("numInputRows"),
                    lp.get("processedRowsPerSecond") or 0.0,
                    lp.get("durationMs", {}).get("triggerExecution"),
                )

    threading.Thread(target=_progress_logger, daemon=True).start()

    # Block on awaitTermination — this PROPAGATES any query errors as
    # exceptions, instead of silently letting isActive flip to False.
    try:
        query.awaitTermination()
        # If we reach here the query terminated cleanly (no exception).
        # For an unbounded Kafka source this only happens via .stop(),
        # so it's not really expected in production.
        log.warning("Streaming query terminated cleanly (no error)")
        return 0
    except KeyboardInterrupt:
        log.info("Interrupted — stopping query gracefully")
        query.stop()
        return 0
    except Exception as e:
        log.error("=" * 72)
        log.error("STREAMING QUERY FAILED — driver will exit with non-zero")
        log.error("=" * 72)
        log.exception("Top-level exception: %s", e)
        # Also dump the underlying query exception if any
        try:
            qe = query.exception()
            if qe is not None:
                log.error("query.exception(): %s", qe)
        except Exception:
            pass
        return 1
    finally:
        try:
            query.stop()
        except Exception:
            pass
        spark.stop()
        log.info("Streaming driver shut down")
