-- ============================================================
-- 10_trino_users_queries.sql
-- Run via Trino CLI / DBeaver / Metabase
-- ============================================================

-- ---- Discovery ----
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.bronze;
SHOW TABLES FROM iceberg.silver;
SHOW TABLES FROM iceberg.gold;

-- ---- Row counts ----
SELECT 'bronze.users_raw'  AS layer, COUNT(*) AS rows FROM iceberg.bronze.users_raw
UNION ALL
SELECT 'silver.users_clean',          COUNT(*) FROM iceberg.silver.users_clean
UNION ALL
SELECT 'gold.users_email_domain',     COUNT(*) FROM iceberg.gold.users_email_domain
UNION ALL
SELECT 'gold.users_daily_changes',    COUNT(*) FROM iceberg.gold.users_daily_changes;

-- ---- Bronze: CDC event mix ----
SELECT op_type, COUNT(*) AS event_count
FROM iceberg.bronze.users_raw
GROUP BY op_type
ORDER BY event_count DESC;

-- ---- Silver: top-10 users ----
SELECT id, name, email, email_domain, source_ts
FROM iceberg.silver.users_clean
ORDER BY id
LIMIT 10;

-- ---- Gold: domain distribution (Metabase pie / bar) ----
SELECT email_domain,
       user_count,
       ROUND(user_count * 100.0 / SUM(user_count) OVER (), 2) AS pct_of_total
FROM iceberg.gold.users_email_domain
ORDER BY user_count DESC;

-- ---- Gold: daily change activity (Metabase line chart) ----
SELECT change_date, op_type, event_count
FROM iceberg.gold.users_daily_changes
ORDER BY change_date, op_type;

-- ---- Freshness: how recent is bronze vs silver? ----
SELECT
    (SELECT MAX(_ingestion_ts)  FROM iceberg.bronze.users_raw)   AS bronze_last_ingest,
    (SELECT MAX(silver_built_at) FROM iceberg.silver.users_clean) AS silver_last_build,
    (SELECT MAX(gold_built_at)  FROM iceberg.gold.users_email_domain) AS gold_last_build;
