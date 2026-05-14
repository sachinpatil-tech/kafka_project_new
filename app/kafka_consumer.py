"""
================================================================
 kafka_consumer.py — Kafka (Debezium CDC) → Iceberg Bronze
================================================================
 PySpark Structured Streaming (or batch) reads CDC messages from
 Kafka topic 'pgb.public.users' (Debezium envelope), explodes the
 envelope, and writes to Iceberg Bronze layer in MinIO with
 Nessie catalog registration.

 Debezium envelope structure:
   {
     "before": { "id": 1, "name": "...", "email": "..." } | null,
     "after":  { "id": 1, "name": "...", "email": "..." } | null,
     "op":     "c" (create) | "u" (update) | "d" (delete) | "r" (snapshot),
     "ts_ms":  1715000000000,
     "source": { "db": "mydb", "schema": "public", "table": "users", ... }
   }

 Bronze tables:
   - lakehouse.bronze.users_raw   (full CDC events; append-only history)
================================================================
"""
from datetime import datetime
from typing import Optional

from app import config
from app.utils import get_logger, print_section

log = get_logger("consumer")


# ----------------------------------------------------------------
# SCHEMAS — Debezium envelope for users
# ----------------------------------------------------------------
def _build_schemas():
    """Build Iceberg schemas lazily (PySpark import only here)."""
    from pyspark.sql.types import (
        StructType, StructField, StringType, LongType, IntegerType
    )

    # Inner row schema — mydb.public.users (id, name, email)
    USER_ROW = StructType([
        StructField("id",    IntegerType()),
        StructField("name",  StringType()),
        StructField("email", StringType()),
    ])

    # Debezium "source" block (subset — we keep useful fields only)
    SOURCE_BLOCK = StructType([
        StructField("version",  StringType()),
        StructField("connector",StringType()),
        StructField("name",     StringType()),
        StructField("ts_ms",    LongType()),
        StructField("db",       StringType()),
        StructField("schema",   StringType()),
        StructField("table",    StringType()),
        StructField("lsn",      LongType()),
        StructField("txId",     LongType()),
    ])

    # Full Debezium CDC envelope
    SCHEMA_USERS_CDC = StructType([
        StructField("before", USER_ROW),
        StructField("after",  USER_ROW),
        StructField("source", SOURCE_BLOCK),
        StructField("op",     StringType()),
        StructField("ts_ms",  LongType()),
    ])

    # Topic name → (schema, iceberg target table)
    return {
        "pgb.public.users": (SCHEMA_USERS_CDC, "lakehouse.bronze.users_raw"),
    }


# ----------------------------------------------------------------
# KAFKA AUTH OPTIONS
# ----------------------------------------------------------------
def _get_kafka_options() -> dict:
    """Kafka source options. SASL fields only added when actually required."""
    opts = {
        "kafka.bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "kafka.security.protocol": config.KAFKA_SECURITY_PROTOCOL,
        "startingOffsets":         "earliest",
        "failOnDataLoss":          "false",
    }
    # PLAINTEXT / SSL ke liye SASL options skip karo (warna broker handshake fail karta hai)
    if config.KAFKA_SECURITY_PROTOCOL.startswith("SASL"):
        jaas_config = (
            f'org.apache.kafka.common.security.scram.ScramLoginModule required '
            f'username="{config.KAFKA_USERNAME}" password="{config.KAFKA_PASSWORD}";'
        )
        opts["kafka.sasl.mechanism"]   = config.KAFKA_SASL_MECHANISM
        opts["kafka.sasl.jaas.config"] = jaas_config
    return opts


# ----------------------------------------------------------------
# SPARK SESSION BUILDER
# ----------------------------------------------------------------
def _create_spark_session():
    """Create Spark session with Iceberg + Nessie + MinIO wiring."""
    from pyspark.sql import SparkSession

    log.info("Creating Spark session")
    log.info("  Nessie:    %s", config.NESSIE_URI)
    log.info("  MinIO:     %s", config.S3_ENDPOINT)
    log.info("  Warehouse: %s", config.ICEBERG_WAREHOUSE)

    spark = (
        SparkSession.builder
        .appName("LakehouseUsersCDCConsumer")
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
        # S3 FileIO for MinIO
        .config("spark.sql.catalog.lakehouse.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.lakehouse.s3.endpoint",          config.S3_ENDPOINT)
        .config("spark.sql.catalog.lakehouse.s3.path-style-access", "true")
        .config("spark.sql.catalog.lakehouse.s3.access-key-id",     config.S3_ACCESS_KEY)
        .config("spark.sql.catalog.lakehouse.s3.secret-access-key", config.S3_SECRET_KEY)
        .config("spark.sql.catalog.lakehouse.s3.region",            config.S3_REGION)
        # Hadoop S3A fallback
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

    log.info("Spark session ready")
    return spark


# ----------------------------------------------------------------
# CDC PARSING HELPER  (envelope → flat bronze row)
# ----------------------------------------------------------------
def _parse_cdc(df_raw, schema):
    """Debezium envelope ko flat bronze schema mein convert karta hai."""
    from pyspark.sql.functions import (
        col, from_json, current_timestamp, lit, when, coalesce
    )

    df = (
        df_raw
        .selectExpr(
            "CAST(key AS STRING)   as kafka_key",
            "CAST(value AS STRING) as json_value",
            "topic                  as kafka_topic",
            "partition              as kafka_partition",
            "offset                 as kafka_offset",
            "timestamp              as kafka_timestamp",
        )
        # Tombstone messages (delete markers with null value) ko skip karo
        .filter(col("json_value").isNotNull())
        .select(
            "kafka_key", "kafka_topic", "kafka_partition",
            "kafka_offset", "kafka_timestamp",
            from_json(col("json_value"), schema).alias("payload"),
        )
        .select(
            col("kafka_key"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("payload.op").alias("op"),
            col("payload.ts_ms").alias("source_ts_ms"),
            col("payload.source.db").alias("source_db"),
            col("payload.source.schema").alias("source_schema"),
            col("payload.source.table").alias("source_table"),
            col("payload.source.lsn").alias("source_lsn"),
            # `before` block (for u/d)
            col("payload.before.id").alias("before_id"),
            col("payload.before.name").alias("before_name"),
            col("payload.before.email").alias("before_email"),
            # `after` block (for c/u/r)
            col("payload.after.id").alias("after_id"),
            col("payload.after.name").alias("after_name"),
            col("payload.after.email").alias("after_email"),
        )
        # Effective row id — `after.id` for c/u/r, else `before.id` for d
        .withColumn("id", coalesce(col("after_id"), col("before_id")))
        .withColumn("op_type", when(col("op") == "c", "INSERT")
                                .when(col("op") == "u", "UPDATE")
                                .when(col("op") == "d", "DELETE")
                                .when(col("op") == "r", "SNAPSHOT")
                                .otherwise("UNKNOWN"))
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source", lit("debezium-kafka"))
    )
    return df


# ----------------------------------------------------------------
# PROCESSING FUNCTIONS
# ----------------------------------------------------------------
def _process_batch(spark, kafka_options, topic, schema, iceberg_table) -> int:
    """One topic ka batch — read all available, append to Bronze."""

    log.info("Processing: %s → %s", topic, iceberg_table)

    df_raw = (
        spark.read
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .load()
    )

    msg_count = df_raw.count()
    if msg_count == 0:
        log.warning("No messages in topic '%s'", topic)
        return 0

    log.info("Found %d messages in Kafka", msg_count)

    df_parsed = _parse_cdc(df_raw, schema)
    row_count = df_parsed.count()
    log.info("Parsed %d CDC events", row_count)

    # APPEND mode — bronze CDC history immutable rakhni hai.
    # First run me table create karna padega, baad me append.
    try:
        df_parsed.writeTo(iceberg_table).append()
    except Exception as e:
        log.warning("Append failed (%s) — creating table on first run", e)
        (df_parsed.writeTo(iceberg_table)
            .tableProperty("format-version", "2")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .createOrReplace())

    log.info("✓ Wrote %d rows to %s", row_count, iceberg_table)
    return row_count


def _process_streaming(spark, kafka_options, topic, schema, iceberg_table,
                       checkpoint_dir: str):
    """One topic ka continuous stream — appends Debezium CDC to Bronze."""
    import os

    log.info("Starting stream: %s → %s", topic, iceberg_table)

    df_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", topic)
        .load()
    )

    df_parsed = _parse_cdc(df_stream, schema)

    checkpoint = os.path.join(checkpoint_dir, topic.replace(".", "_"))
    query = (
        df_parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("fanout-enabled", "true")
        .trigger(processingTime="30 seconds")
        .toTable(iceberg_table)
    )
    return query


# ----------------------------------------------------------------
# PUBLIC ENTRY POINT
# ----------------------------------------------------------------
def run(mode: str = "batch",
        topic: Optional[str] = None,
        checkpoint_dir: str = "/tmp/kafka-iceberg-checkpoints") -> int:
    """
    Kafka → Iceberg Bronze CDC consumer.

    Args:
        mode: "batch" or "streaming"
        topic: specific topic (default: from KAFKA_TOPIC env)
        checkpoint_dir: for streaming mode
    Returns:
        0 on success, non-zero on failure
    """
    print_section(f"KAFKA CDC CONSUMER — {mode.upper()} MODE")
    log.info("Started at: %s", datetime.now().isoformat())

    try:
        topics_config = _build_schemas()
    except ImportError as e:
        log.error("PySpark not installed: %s", e)
        log.error("Install: pip install pyspark==3.5.4")
        return 1

    if topic is None and config.KAFKA_TOPIC:
        topic = config.KAFKA_TOPIC

    if topic:
        if topic not in topics_config:
            log.error("Unknown topic: %s", topic)
            log.error("Available: %s", list(topics_config.keys()))
            return 1
        topics_config = {topic: topics_config[topic]}

    spark = _create_spark_session()
    kafka_options = _get_kafka_options()

    if mode == "batch":
        total = 0
        for tp, (schema, table) in topics_config.items():
            try:
                total += _process_batch(spark, kafka_options, tp, schema, table)
            except Exception as e:
                log.error("Topic %s failed: %s", tp, e)

        print_section(f"BATCH COMPLETE — {total} total rows", char="=")

        log.info("Verifying...")
        for tp, (_, table) in topics_config.items():
            try:
                count = spark.table(table).count()
                log.info("  %s: %d rows", table, count)
            except Exception:
                log.warning("  %s: table not created", table)

        spark.stop()
        return 0

    elif mode == "streaming":
        queries = []
        for tp, (schema, table) in topics_config.items():
            try:
                q = _process_streaming(spark, kafka_options, tp, schema,
                                       table, checkpoint_dir)
                queries.append(q)
                log.info("Started streaming for %s", tp)
            except Exception as e:
                log.error("Stream start failed for %s: %s", tp, e)

        print_section(f"STREAMING — {len(queries)} active", char="=")
        log.info("Press Ctrl+C to stop")

        try:
            for q in queries:
                q.awaitTermination()
        except KeyboardInterrupt:
            log.info("Stopping streams...")
            for q in queries:
                q.stop()

        spark.stop()
        return 0

    else:
        log.error("Invalid mode: %s (use 'batch' or 'streaming')", mode)
        return 1
