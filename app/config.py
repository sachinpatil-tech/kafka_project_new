"""
================================================================
 config.py — Centralized environment-based configuration
================================================================
 Saare environment variables ek hi jagah. Har module yahan se
 config padhta hai. Hardcoded values nahi — sab override-able.

 NEW: Debezium CDC topic 'pgb.public.users' ke liye configure kiya hai.
 Source DB : dvdrental.actor  → transform → mydb.public.users
 Debezium publish karta hai 'pgb.public.users' Kafka topic pe.
================================================================
"""
import os


# ----------------------------------------------------------------
# KAFKA CONFIGURATION  (Debezium CDC topic)
# ----------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "my-cluster-kafka-bootstrap.lakehouse-ingest.svc.cluster.local:9092"
)
# Debezium ka default topic naming: <server.name>.<schema>.<table>
KAFKA_TOPIC             = os.environ.get("KAFKA_TOPIC", "pgb.public.users")
KAFKA_SECURITY_PROTOCOL = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
KAFKA_SASL_MECHANISM    = os.environ.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
KAFKA_USERNAME          = os.environ.get("KAFKA_USERNAME", "app-user")
KAFKA_PASSWORD          = os.environ.get("KAFKA_PASSWORD", "bwDqbYGrgC2AKOMuthoUu7Ckkj8tNjtB")


# ----------------------------------------------------------------
# POSTGRES (Source/Target) CONFIGURATION
# ----------------------------------------------------------------
# Source DB:  dvdrental  (table: actor)
# Target DB:  mydb       (table: public.users)
PG_HOST            = os.environ.get("PG_HOST", "postgresql.lakehouse-ingest.svc.cluster.local")
PG_PORT            = int(os.environ.get("PG_PORT", "5432"))
PG_USER            = os.environ.get("PG_USER", "postgres")
PG_PASSWORD        = os.environ.get("PG_PASSWORD", "postgres")
PG_SOURCE_DB       = os.environ.get("PG_SOURCE_DB", "dvdrental")
PG_SOURCE_TABLE    = os.environ.get("PG_SOURCE_TABLE", "public.actor")
PG_TARGET_DB       = os.environ.get("PG_TARGET_DB", "mydb")
PG_TARGET_TABLE    = os.environ.get("PG_TARGET_TABLE", "public.users")


# ----------------------------------------------------------------
# MINIO / S3 CONFIGURATION
# ----------------------------------------------------------------
S3_ENDPOINT   = os.environ.get(
    "S3_ENDPOINT",
    "http://minio-api.lakehouse-data.svc.cluster.local:9000"
)
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "MyStr0ngP@ssw0rd123")
S3_REGION     = os.environ.get("S3_REGION", "us-east-1")


# ----------------------------------------------------------------
# ICEBERG / NESSIE CONFIGURATION
# ----------------------------------------------------------------
ICEBERG_WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE",
    "s3a://lakehouse-warehouse/warehouse"
)
NESSIE_URI        = os.environ.get(
    "NESSIE_URI",
    "http://nessie.lakehouse-catalog.svc:19120/api/v2"
)
NESSIE_REF        = os.environ.get("NESSIE_REF", "main")


# ----------------------------------------------------------------
# TRINO CONFIGURATION
# ----------------------------------------------------------------
TRINO_HOST        = os.environ.get(
    "TRINO_HOST",
    "trino.lakehouse-catalog.svc.cluster.local"
)
TRINO_PORT        = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER        = os.environ.get("TRINO_USER", "admin")
TRINO_CATALOG     = os.environ.get("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA      = os.environ.get("TRINO_SCHEMA", "bronze")
TRINO_HTTP_SCHEME = os.environ.get("TRINO_HTTP_SCHEME", "http")


# ----------------------------------------------------------------
# APPLICATION CONSTANTS  (Lakehouse multi-layer tables)
# ----------------------------------------------------------------
BRONZE_TABLES = [
    "users_raw",          # raw CDC envelope from Debezium
]
SILVER_TABLES = [
    "users_clean",        # latest state per id, deduped, cleaned
]
GOLD_TABLES = [
    "users_email_domain", # aggregate: count of users per email domain
    "users_daily_changes" # aggregate: daily inserts/updates/deletes
]


def print_config():
    """Startup pe current configuration print karo (debugging ke liye)."""
    print("=" * 72)
    print(" CONFIGURATION ".center(72, "="))
    print("=" * 72)
    print(f"  KAFKA_BOOTSTRAP_SERVERS : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  KAFKA_TOPIC             : {KAFKA_TOPIC}")
    print(f"  PG_SOURCE               : {PG_HOST}:{PG_PORT}/{PG_SOURCE_DB}.{PG_SOURCE_TABLE}")
    print(f"  PG_TARGET               : {PG_HOST}:{PG_PORT}/{PG_TARGET_DB}.{PG_TARGET_TABLE}")
    print(f"  S3_ENDPOINT             : {S3_ENDPOINT}")
    print(f"  ICEBERG_WAREHOUSE       : {ICEBERG_WAREHOUSE}")
    print(f"  NESSIE_URI              : {NESSIE_URI}")
    print(f"  TRINO_HOST              : {TRINO_HOST}:{TRINO_PORT}")
    print(f"  TRINO_CATALOG           : {TRINO_CATALOG}")
    print(f"  TRINO_SCHEMA            : {TRINO_SCHEMA}")
    print("=" * 72)
