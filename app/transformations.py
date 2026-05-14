"""
================================================================
 transformations.py — Silver & Gold layer builders
================================================================
 Bronze (raw CDC events, append-only)
     ↓  silver_clean
 Silver (latest state per id, deduped, cleansed)
     ↓  gold_agg
 Gold  (aggregated KPIs: email-domain, daily change counts)

 Entry points:
   build_silver(spark) → int (rows written)
   build_gold(spark)   → int (rows written)
   run(layer="silver"|"gold"|"all") → int (exit code)
================================================================
"""
from datetime import datetime
from typing import Optional

from app import config
from app.utils import get_logger, print_section

log = get_logger("transform")

BRONZE_USERS = "lakehouse.bronze.users_raw"
SILVER_USERS = "lakehouse.silver.users_clean"
GOLD_DOMAIN  = "lakehouse.gold.users_email_domain"
GOLD_CHANGES = "lakehouse.gold.users_daily_changes"


# ----------------------------------------------------------------
# SPARK SESSION (same wiring as kafka_consumer)
# ----------------------------------------------------------------
def _create_spark_session():
    from pyspark.sql import SparkSession

    log.info("Creating Spark session for transformations")
    spark = (
        SparkSession.builder
        .appName("LakehouseUsersTransform")
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
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    return spark


# ----------------------------------------------------------------
# SILVER — latest state per id (no deletes), basic cleansing
# ----------------------------------------------------------------
def build_silver(spark) -> int:
    """
    Bronze ke saare CDC events ko collapse karke har id ka
    LATEST non-deleted state nikalo → Silver users_clean.
    """
    print_section("SILVER BUILD — users_clean")

    bronze = spark.table(BRONZE_USERS)
    log.info("Bronze rows: %d", bronze.count())

    # Window function: per id, pick row with max source_ts_ms (latest CDC)
    bronze.createOrReplaceTempView("v_bronze_users")
    silver_df = spark.sql(f"""
        WITH ranked AS (
            SELECT
                id,
                op,
                op_type,
                source_ts_ms,
                source_lsn,
                COALESCE(after_name,  before_name)  AS name,
                COALESCE(after_email, before_email) AS email,
                _ingestion_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY source_ts_ms DESC, source_lsn DESC NULLS LAST
                ) AS rn
            FROM v_bronze_users
            WHERE id IS NOT NULL
        )
        SELECT
            id,
            TRIM(name)                            AS name,
            LOWER(TRIM(email))                    AS email,
            CASE
                WHEN email IS NOT NULL AND email LIKE '%@%'
                THEN LOWER(SPLIT(TRIM(email), '@')[1])
                ELSE NULL
            END                                   AS email_domain,
            source_ts_ms,
            CAST(source_ts_ms / 1000 AS TIMESTAMP) AS source_ts,
            _ingestion_ts                         AS bronze_ingested_at,
            current_timestamp()                   AS silver_built_at
        FROM ranked
        WHERE rn = 1
          AND op_type <> 'DELETE'        -- deleted rows ko silver mein nahi rakhna
    """)

    row_count = silver_df.count()
    log.info("Silver rows: %d", row_count)

    (silver_df.writeTo(SILVER_USERS)
        .tableProperty("format-version", "2")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .createOrReplace())

    log.info("✓ Wrote %d rows to %s", row_count, SILVER_USERS)
    return row_count


# ----------------------------------------------------------------
# GOLD — aggregates for BI dashboards
# ----------------------------------------------------------------
def build_gold(spark) -> int:
    """
    Silver se 2 gold tables:
      1. users_email_domain  — count of users per email domain
      2. users_daily_changes — daily insert/update/delete tally from Bronze
    """
    print_section("GOLD BUILD — domain + daily change tallies")

    # ---- Gold 1: email-domain distribution ----
    domain_df = spark.sql(f"""
        SELECT
            email_domain,
            COUNT(*)                              AS user_count,
            current_timestamp()                   AS gold_built_at
        FROM {SILVER_USERS}
        WHERE email_domain IS NOT NULL
        GROUP BY email_domain
        ORDER BY user_count DESC
    """)
    d1 = domain_df.count()
    (domain_df.writeTo(GOLD_DOMAIN)
        .tableProperty("format-version", "2")
        .createOrReplace())
    log.info("✓ %s: %d rows", GOLD_DOMAIN, d1)

    # ---- Gold 2: daily change tally (from full Bronze history) ----
    changes_df = spark.sql(f"""
        SELECT
            DATE(CAST(source_ts_ms / 1000 AS TIMESTAMP))   AS change_date,
            op_type,
            COUNT(*)                                       AS event_count,
            current_timestamp()                            AS gold_built_at
        FROM {BRONZE_USERS}
        WHERE source_ts_ms IS NOT NULL
        GROUP BY DATE(CAST(source_ts_ms / 1000 AS TIMESTAMP)), op_type
        ORDER BY change_date DESC, op_type
    """)
    d2 = changes_df.count()
    (changes_df.writeTo(GOLD_CHANGES)
        .tableProperty("format-version", "2")
        .createOrReplace())
    log.info("✓ %s: %d rows", GOLD_CHANGES, d2)

    return d1 + d2


# ----------------------------------------------------------------
# PUBLIC ENTRY POINT
# ----------------------------------------------------------------
def run(layer: Optional[str] = "all") -> int:
    """
    Run silver and/or gold transformations.

    Args:
        layer: "silver" | "gold" | "all"
    Returns:
        0 on success, non-zero on failure
    """
    print_section(f"TRANSFORM — layer={layer}")
    log.info("Started at: %s", datetime.now().isoformat())

    layer = (layer or "all").lower()
    if layer not in {"silver", "gold", "all"}:
        log.error("Invalid layer: %s", layer)
        return 1

    spark = _create_spark_session()
    try:
        if layer in ("silver", "all"):
            build_silver(spark)
        if layer in ("gold", "all"):
            build_gold(spark)
        log.info("✓ Transform complete")
        return 0
    except Exception as e:
        log.exception("Transform failed: %s", e)
        return 2
    finally:
        spark.stop()
