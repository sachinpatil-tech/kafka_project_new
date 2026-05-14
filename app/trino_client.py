"""
================================================================
 trino_client.py — Query Lakehouse (Bronze/Silver/Gold) via Trino
================================================================
 Sections:
   counts   — discovery + row counts across bronze/silver/gold
   bronze   — raw CDC event analytics (users_raw)
   silver   — current users state (users_clean)
   gold     — aggregated KPIs (email-domain, daily changes)

 Entry point: run(section=None | "counts" | "bronze" | "silver" | "gold")
================================================================
"""
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from app import config
from app.utils import get_logger, print_section, print_subsection

log = get_logger("trino")


BRONZE_USERS = "iceberg.bronze.users_raw"
SILVER_USERS = "iceberg.silver.users_clean"
GOLD_DOMAIN  = "iceberg.gold.users_email_domain"
GOLD_CHANGES = "iceberg.gold.users_daily_changes"


# ----------------------------------------------------------------
# CONNECTION
# ----------------------------------------------------------------
@contextmanager
def _connect():
    try:
        from trino.dbapi import connect
    except ImportError:
        log.error("trino package missing. Install: pip install trino")
        raise

    log.info("Trino: %s://%s:%d (catalog=%s, schema=%s)",
             config.TRINO_HTTP_SCHEME, config.TRINO_HOST, config.TRINO_PORT,
             config.TRINO_CATALOG, config.TRINO_SCHEMA)

    conn = connect(
        host=config.TRINO_HOST,
        port=config.TRINO_PORT,
        user=config.TRINO_USER,
        catalog=config.TRINO_CATALOG,
        schema=config.TRINO_SCHEMA,
        http_scheme=config.TRINO_HTTP_SCHEME,
    )
    try:
        yield conn
    finally:
        conn.close()


def _query(conn, sql: str):
    import pandas as pd
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


def _show(df, title: str, max_rows: int = 25):
    from tabulate import tabulate
    print_subsection(title)
    if df is None or df.empty:
        print("(no rows)")
        return
    print(tabulate(df.head(max_rows), headers="keys", tablefmt="psql",
                   showindex=False))
    if len(df) > max_rows:
        print(f"... ({len(df) - max_rows} more rows)")


def _run_and_show(conn, sql: str, title: str, max_rows: int = 25):
    try:
        from trino.exceptions import TrinoQueryError, TrinoUserError
        try:
            df = _query(conn, sql)
            _show(df, title, max_rows=max_rows)
            return df
        except (TrinoUserError, TrinoQueryError) as e:
            msg = e.message if hasattr(e, "message") else str(e)
            log.warning("Query failed [%s]: %s", title, msg[:200])
            return None
    except Exception as e:
        log.error("Unexpected error [%s]: %s", title, e)
        return None


def _table_exists(conn, schema: str, table: str) -> bool:
    try:
        df = _query(conn, f"SHOW TABLES FROM iceberg.{schema} LIKE '{table}'")
        return df is not None and not df.empty
    except Exception:
        return False


# ----------------------------------------------------------------
# SECTION: counts
# ----------------------------------------------------------------
def _section_counts(conn):
    print_section("SECTION 1: COUNTS — Discovery across layers")

    for schema in ("bronze", "silver", "gold"):
        _run_and_show(conn, f"SHOW TABLES FROM iceberg.{schema}",
                      f"Tables in {schema}")

    for fq in (BRONZE_USERS, SILVER_USERS, GOLD_DOMAIN, GOLD_CHANGES):
        schema, table = fq.split(".")[1], fq.split(".")[2]
        if _table_exists(conn, schema, table):
            _run_and_show(conn,
                f"SELECT COUNT(*) AS row_count FROM {fq}",
                f"Row count — {fq}")
        else:
            print(f"\n--- {fq} ---  (does not exist; skipping)")


# ----------------------------------------------------------------
# SECTION: bronze — raw CDC analytics
# ----------------------------------------------------------------
def _section_bronze(conn):
    print_section("SECTION 2: BRONZE — Raw CDC events (users_raw)")

    if not _table_exists(conn, "bronze", "users_raw"):
        log.warning("%s missing — run consumer first", BRONZE_USERS)
        return

    _run_and_show(conn, f"""
        SELECT op_type,
               COUNT(*)                                       AS event_count,
               MIN(CAST(source_ts_ms / 1000 AS TIMESTAMP))    AS first_event,
               MAX(CAST(source_ts_ms / 1000 AS TIMESTAMP))    AS last_event
        FROM {BRONZE_USERS}
        GROUP BY op_type
        ORDER BY event_count DESC
    """, "CDC event distribution by operation type")

    _run_and_show(conn, f"""
        SELECT DATE(CAST(source_ts_ms / 1000 AS TIMESTAMP)) AS change_date,
               op_type,
               COUNT(*) AS event_count
        FROM {BRONZE_USERS}
        WHERE source_ts_ms IS NOT NULL
        GROUP BY DATE(CAST(source_ts_ms / 1000 AS TIMESTAMP)), op_type
        ORDER BY change_date DESC, op_type
        LIMIT 30
    """, "Daily change activity (last 30 days)")

    _run_and_show(conn, f"""
        SELECT id, op_type, after_name, after_email,
               CAST(source_ts_ms / 1000 AS TIMESTAMP) AS source_ts,
               _ingestion_ts
        FROM {BRONZE_USERS}
        ORDER BY source_ts_ms DESC
        LIMIT 20
    """, "Latest 20 CDC events")


# ----------------------------------------------------------------
# SECTION: silver — current users state
# ----------------------------------------------------------------
def _section_silver(conn):
    print_section("SECTION 3: SILVER — Current users state (users_clean)")

    if not _table_exists(conn, "silver", "users_clean"):
        log.warning("%s missing — run silver build first", SILVER_USERS)
        return

    _run_and_show(conn, f"""
        SELECT
            COUNT(*)                          AS total_users,
            COUNT(DISTINCT email_domain)      AS unique_domains,
            MIN(source_ts)                    AS earliest_record,
            MAX(source_ts)                    AS latest_record,
            MAX(silver_built_at)              AS silver_last_built
        FROM {SILVER_USERS}
    """, "Silver KPIs")

    _run_and_show(conn, f"""
        SELECT id, name, email, email_domain, source_ts
        FROM {SILVER_USERS}
        ORDER BY id ASC
        LIMIT 20
    """, "Sample users (first 20 by id)")

    _run_and_show(conn, f"""
        SELECT
            SUM(CASE WHEN name  IS NULL THEN 1 ELSE 0 END) AS null_names,
            SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_emails,
            SUM(CASE WHEN email NOT LIKE '%@%' THEN 1 ELSE 0 END) AS bad_emails,
            COUNT(*)                                      AS total_rows
        FROM {SILVER_USERS}
    """, "Data quality checks")


# ----------------------------------------------------------------
# SECTION: gold — aggregates for BI
# ----------------------------------------------------------------
def _section_gold(conn):
    print_section("SECTION 4: GOLD — Aggregated KPIs")

    if _table_exists(conn, "gold", "users_email_domain"):
        _run_and_show(conn, f"""
            SELECT email_domain, user_count,
                   ROUND(user_count * 100.0 / SUM(user_count) OVER (), 2) AS pct
            FROM {GOLD_DOMAIN}
            ORDER BY user_count DESC
            LIMIT 20
        """, "Top 20 email domains by user count")

    if _table_exists(conn, "gold", "users_daily_changes"):
        _run_and_show(conn, f"""
            SELECT change_date, op_type, event_count
            FROM {GOLD_CHANGES}
            ORDER BY change_date DESC, op_type
            LIMIT 30
        """, "Daily change tally")


# ----------------------------------------------------------------
# DISPATCH
# ----------------------------------------------------------------
_SECTIONS = {
    "counts": _section_counts,
    "bronze": _section_bronze,
    "silver": _section_silver,
    "gold":   _section_gold,
}


def run(section: Optional[str] = None) -> int:
    """
    Run Trino analytics queries.

    Args:
        section: "counts" | "bronze" | "silver" | "gold" | None (all)
    Returns:
        0 on success, non-zero on failure
    """
    print_section("TRINO LAKEHOUSE CLIENT — users pipeline")
    log.info("Started at: %s", datetime.now().isoformat())

    try:
        with _connect() as conn:
            df = _query(conn, "SHOW CATALOGS")
            catalogs = df["Catalog"].tolist() if df is not None and not df.empty else []
            log.info("Available catalogs: %s", catalogs)

            if "iceberg" not in catalogs:
                log.error("'iceberg' catalog missing in Trino")
                return 1

            df = _query(conn, "SHOW SCHEMAS FROM iceberg")
            schemas = df["Schema"].tolist() if df is not None and not df.empty else []
            log.info("Schemas: %s", schemas)

            if section:
                if section not in _SECTIONS:
                    log.error("Unknown section: %s (valid: %s)",
                              section, list(_SECTIONS.keys()))
                    return 1
                _SECTIONS[section](conn)
            else:
                for name, fn in _SECTIONS.items():
                    log.info("========== %s ==========", name)
                    try:
                        fn(conn)
                    except Exception as e:
                        log.error("Section '%s' crashed: %s", name, e)

        log.info("✓ Done")
        return 0

    except Exception as e:
        log.exception("Fatal: %s", e)
        return 2
