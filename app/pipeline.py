"""
================================================================
 pipeline.py — End-to-end Lakehouse runner
================================================================
 Chains Bronze → Silver → Gold → Trino verify into a single
 Spark job. Use this when you want one container/job to perform
 the entire pipeline (Tekton-driven deploys).

 Each stage runs guarded — if one fails (e.g. empty Kafka topic),
 the runner logs the failure and continues. Trino verify always
 runs last so logs always end with row counts.

 Entry point: run(topic=None) → exit code
================================================================
"""
from datetime import datetime
from typing import Optional

from app import config
from app.utils import get_logger, print_section

log = get_logger("pipeline")


# ----------------------------------------------------------------
# SHARED SPARK SESSION
# ----------------------------------------------------------------
def _create_spark_session():
    """Single Spark session reused across Bronze + Silver + Gold stages."""
    from pyspark.sql import SparkSession

    log.info("Creating Spark session for full pipeline")
    spark = (
        SparkSession.builder
        .appName("LakehouseUsersFullPipeline")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
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
        .config("spark.hadoop.fs.s3a.endpoint",          config.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        config.S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        config.S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.defaultCatalog", "lakehouse")
        .getOrCreate()
    )
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    return spark


# ----------------------------------------------------------------
# STAGE 1 — Bronze (Kafka CDC batch)
# ----------------------------------------------------------------
def _stage_bronze(spark, topic: str) -> int:
    """Read Kafka, parse Debezium envelope, append/create Bronze table."""
    from app.kafka_consumer import _build_schemas, _get_kafka_options, _parse_cdc

    print_section("STAGE 1/4: BRONZE — Kafka CDC → users_raw")

    topics = _build_schemas()
    if topic not in topics:
        log.error("Unknown topic: %s (available: %s)", topic, list(topics))
        return 0
    schema, iceberg_table = topics[topic]
    kafka_opts = _get_kafka_options()

    df_raw = (
        spark.read.format("kafka").options(**kafka_opts)
        .option("subscribe", topic).load()
    )
    msg_count = df_raw.count()
    log.info("Kafka messages found: %d", msg_count)
    if msg_count == 0:
        log.warning("No messages in topic '%s' — creating empty bronze table", topic)
        # Empty df with proper schema so downstream stages don't crash
        empty_df = _parse_cdc(df_raw, schema)
        (empty_df.writeTo(iceberg_table)
            .tableProperty("format-version", "2")
            .createOrReplace())
        return 0

    df_parsed = _parse_cdc(df_raw, schema)
    row_count = df_parsed.count()
    log.info("Parsed CDC events: %d", row_count)

    try:
        df_parsed.writeTo(iceberg_table).append()
    except Exception as e:
        log.warning("Append failed (%s) — creating table fresh", e)
        (df_parsed.writeTo(iceberg_table)
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .createOrReplace())

    log.info("✓ Bronze wrote %d rows to %s", row_count, iceberg_table)
    return row_count


# ----------------------------------------------------------------
# STAGE 2 — Silver (dedupe + cleanse)
# ----------------------------------------------------------------
def _stage_silver(spark) -> int:
    """Bronze → Silver (latest non-deleted state per id)."""
    from app.transformations import build_silver
    print_section("STAGE 2/4: SILVER — users_clean")
    return build_silver(spark)


# ----------------------------------------------------------------
# STAGE 3 — Gold (aggregates)
# ----------------------------------------------------------------
def _stage_gold(spark) -> int:
    """Silver → Gold (email-domain + daily-changes)."""
    from app.transformations import build_gold
    print_section("STAGE 3/4: GOLD — aggregates")
    return build_gold(spark)


# ----------------------------------------------------------------
# STAGE 4 — Trino verify (separate process, no Spark)
# ----------------------------------------------------------------
def _stage_trino_verify() -> int:
    """Trino se row counts dikhao."""
    from app.trino_client import run as trino_run
    print_section("STAGE 4/4: TRINO VERIFY — row counts")
    return trino_run(section="counts")


# ----------------------------------------------------------------
# PUBLIC ENTRY POINT
# ----------------------------------------------------------------
def run(topic: Optional[str] = None) -> int:
    """
    Full lakehouse pipeline. Each stage is guarded — failures are
    logged but the next stage still runs (so we always end with
    Trino counts in the logs).
    """
    print_section("FULL PIPELINE — Bronze → Silver → Gold → Trino")
    log.info("Started at: %s", datetime.now().isoformat())

    topic = topic or config.KAFKA_TOPIC

    spark = _create_spark_session()
    try:
        try:
            _stage_bronze(spark, topic)
        except Exception as e:
            log.error("Bronze stage failed (continuing): %s", e)

        try:
            _stage_silver(spark)
        except Exception as e:
            log.error("Silver stage failed (continuing): %s", e)

        try:
            _stage_gold(spark)
        except Exception as e:
            log.error("Gold stage failed (continuing): %s", e)
    finally:
        spark.stop()

    try:
        _stage_trino_verify()
    except Exception as e:
        log.error("Trino verify failed: %s", e)

    print_section("✓ FULL PIPELINE COMPLETE")
    return 0
