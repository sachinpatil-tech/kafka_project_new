-- ============================================================
-- 00_source_seed_dvdrental_to_mydb.sql
-- ============================================================
-- Step 1: Create target DB + table (if not exist)
-- Step 2: Transform actor → users  (id, name, email)
-- Step 3: Mark table for Debezium (REPLICA IDENTITY FULL)
-- Run against PostgreSQL (psql -h $PG_HOST -U postgres)
-- ============================================================

-- ---------- 1. Target DB / table ----------
-- Run this connected to the postgres maintenance DB:
--   psql -h $PG_HOST -U postgres -d postgres
--
-- CREATE DATABASE mydb;
\c mydb;

CREATE TABLE IF NOT EXISTS public.users (
    id      INTEGER PRIMARY KEY,
    name    VARCHAR(200) NOT NULL,
    email   VARCHAR(200) NOT NULL
);

-- Debezium needs full row image for u/d events
ALTER TABLE public.users REPLICA IDENTITY FULL;


-- ---------- 2. Transform actor → users via dblink ----------
-- (Run from mydb; requires dblink extension)
CREATE EXTENSION IF NOT EXISTS dblink;

INSERT INTO public.users (id, name, email)
SELECT
    actor_id                                                AS id,
    TRIM(first_name) || ' ' || TRIM(last_name)              AS name,
    LOWER(TRIM(first_name) || '.' || TRIM(last_name)
          || '@example.com')                                AS email
FROM dblink(
    'host=' || current_setting('lakehouse.pg_host', true)
       || ' dbname=dvdrental user=postgres password=postgres',
    'SELECT actor_id, first_name, last_name FROM public.actor'
) AS src(actor_id INT, first_name VARCHAR, last_name VARCHAR)
ON CONFLICT (id) DO NOTHING;

-- ---------- 3. Sanity check ----------
SELECT COUNT(*) AS users_count FROM public.users;
-- Expected: 200 (dvdrental.actor has 200 rows)
