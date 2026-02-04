---
name: postgres-daily-check
description: |
  This skill is designed to perform comprehensive daily health checks on a PostgreSQL database.
  It leverages a suite of specialized SQL queries and analysis logic to provide insights into
  database availability, performance, activity, maintenance, storage, replication, and archiving status.
  The skill aims to identify potential issues such as blocking locks, long-running queries,
  transaction ID wraparound risks, bloat in tables and indexes, connection bottlenecks,
  and archiving failures.
---

# PostgreSQL Daily Check Agent

This skill guides the agent in conducting a thorough daily health check of a PostgreSQL database instance. It executes a series of specialized queries to gather critical metrics and identifies potential issues, presenting them in a structured report.

## Purpose

The primary goal of this skill is to empower the agent to proactively monitor the health and performance of a PostgreSQL database, alerting users to anomalies or potential problems before they escalate. It acts as an automated DBA, performing routine inspections efficiently.

## Core Capabilities

The agent performs checks across several key areas:

*   **Availability & Health**: Verifying database responsiveness and detecting critical errors like invalid indexes or XID wraparound risks.
*   **Performance & Activity**: Monitoring active sessions, long-running queries, cache efficiency, transaction rollback rates, and identifying application hotspots.
*   **Replication & Archiving**: Ensuring data consistency and recoverability by checking replication lag, slot status, and WAL archiving health.
*   **Maintenance & Storage**: Analyzing disk usage, identifying bloat in tables and indexes, and checking autovacuum activity.

## Workflow

When activated, this skill executes a predefined sequence of checks. Each check involves calling a specialized script (`run_postgres_check.sh`) which executes specific SQL queries against the target PostgreSQL database. The results are then analyzed by the agent, and a comprehensive Markdown report is generated.

## Available Skills (Detailed Definitions)

Each item below represents a callable skill within the `run_postgres_check.sh` script, designed to return structured JSON output.

---

### 1. Core Health & Availability

#### Skill: `get_invalid_indexes`

-   **Description**: Checks for any indexes that are in an invalid state. Invalid indexes can cause DML operations to fail or block.
-   **Usage**: `./run_postgres_check.sh get_invalid_indexes`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_invalid_indexes",
      "status": "success",
      "data": [
        {"index_name": "idx_my_invalid_idx", "schema_name": "public"}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports ERROR if any invalid indexes are found.

#### Skill: `get_xid_wraparound_risk`

-   **Description**: Monitors the age of transaction IDs (XID) for each database to detect potential transaction ID wraparound issues, which can lead to database shutdown if not addressed.
-   **Usage**: `./run_postgres_check.sh get_xid_wraparound_risk`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_xid_wraparound_risk",
      "status": "success",
      "data": [
        {"datname": "postgres", "xid_age": 1234567, "percentage_used": 0.05}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports CRITICAL ERROR if XID age is very high (>85% of limit), WARNING if high (>70% of limit), otherwise INFO.

#### Skill: `get_blocking_locks`

-   **Description**: Identifies active blocking lock situations in the database, where one session is holding a lock that another session is waiting for.
-   **Usage**: `./run_postgres_check.sh get_blocking_locks`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_blocking_locks",
      "status": "success",
      "data": [
        {"waiting_pid": 123, "waiting_user": "app_user", "blocking_pid": 456, "blocking_user": "db_admin"}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports ERROR if any blocking locks are found.

---

### 2. Replication & Archiving

#### Skill: `get_replication_slots`

-   **Description**: Checks the status of logical and physical replication slots, reporting if any are inactive and causing WAL retention.
-   **Usage**: `./run_postgres_check.sh get_replication_slots`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_replication_slots",
      "status": "success",
      "data": [
        {"slot_name": "myslot", "plugin": "pgoutput", "active": true, "restart_lsn_lag_bytes": 0}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports ERROR if inactive slots are found, INFO otherwise.

#### Skill: `get_wal_archiver_status`

-   **Description**: Provides status of WAL archiving, including counts of archived/failed WALs, last archived/failed time, and current WAL directory size.
-   **Usage**: `./run_postgres_check.sh get_wal_archiver_status`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_wal_archiver_status",
      "status": "success",
      "data": [
        {"archived_count": 100, "failed_count": 0, "wal_directory_size": "1 GB"}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports ERROR if `failed_count` is greater than 0, INFO otherwise.

#### Skill: `get_replication_status`

-   **Description**: Checks the status and lag of streaming replication from the primary perspective.
-   **Usage**: `./run_postgres_check.sh get_replication_status`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_replication_status",
      "status": "success",
      "data": [
        {"application_name": "replica1", "state": "streaming", "replay_lag_bytes": 1024}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports WARNING/ERROR if `replay_lag_bytes` exceeds thresholds (100MB/1GB).

---

### 3. Performance & Activity

#### Skill: `get_large_unused_indexes`

-   **Description**: Identifies large indexes (over 10MB) that have not been used (`idx_scan = 0`), which might be candidates for removal to save space and improve write performance.
-   **Usage**: `./run_postgres_check.sh get_large_unused_indexes`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_large_unused_indexes",
      "status": "success",
      "data": [
        {"schemaname": "public", "table_name": "my_table", "index_name": "idx_unused", "index_size": "15 MB"}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports WARNING if any large unused indexes are found, INFO otherwise.

#### Skill: `get_long_running_queries`

-   **Description**: Finds currently active SQL queries that have been running longer than a specified threshold (default 5 minutes).
-   **Usage**: `./run_postgres_check.sh get_long_running_queries [threshold_minutes]`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_long_running_queries",
      "status": "success",
      "data": [
        {"pid": 123, "usename": "app_user", "duration": "00:06:30", "query": "SELECT ... FROM ..."}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports WARNING if any long-running queries are found.

#### Skill: `get_idle_in_transaction_sessions`

-   **Description**: Detects sessions that are "idle in transaction" for longer than a specified threshold (default 1 minute), which can hold locks and prevent VACUUM.
-   **Usage**: `./run_postgres_check.sh get_idle_in_transaction_sessions [threshold_minutes]`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_idle_in_transaction_sessions",
      "status": "success",
      "data": [
        {"pid": 456, "usename": "app_user", "transaction_duration": "00:01:45", "query": "BEGIN; UPDATE ..."}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports WARNING if any idle-in-transaction sessions are found.

#### Skill: `get_connection_usage`

-   **Description**: Reports the current number of active connections and the configured maximum connections, calculating the usage percentage.
-   **Usage**: `./run_postgres_check.sh get_connection_usage`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_connection_usage",
      "status": "success",
      "data": [
        {"used_connections": 10, "max_connections": 100}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports ERROR if usage > 95%, WARNING if > 80%, OK otherwise.

#### Skill: `get_cache_hit_rate`

-   **Description**: Calculates the block cache hit rate for the current database. A low hit rate (below 99%) indicates potential memory pressure or inefficient queries.
-   **Usage**: `./run_postgres_check.sh get_cache_hit_rate`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_cache_hit_rate",
      "status": "success",
      "data": [
        {"datname": "postgres", "hit_rate_percentage": 99.5}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports WARNING if hit rate is below 99%, OK otherwise.

#### Skill: `get_rollback_rate`

-   **Description**: Calculates the transaction rollback rate for each database. A high rollback rate (e.g., >5%) can indicate application logic errors.
-   **Usage**: `./run_postgres_check.sh get_rollback_rate`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_rollback_rate",
      "status": "success",
      "data": [
        {"datname": "postgres", "xact_commit": 1000, "xact_rollback": 10, "rollback_percentage": 0.99}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports WARNING if rollback rate is above 5%, INFO otherwise.

#### Skill: `get_top_sql_by_time`

-   **Description**: Retrieves the top 5 SQL queries by total execution time from `pg_stat_statements`. Requires the `pg_stat_statements` extension to be enabled.
-   **Usage**: `./run_postgres_check.sh get_top_sql_by_time`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_top_sql_by_time",
      "status": "success",
      "data": [
        {"total_minutes": 10.5, "avg_ms": 12.3, "calls": 500, "query": "SELECT ..."}
      ]
    }
    ```
-   **Analysis Logic Summary**: Lists the top queries as INFO. Note: Agent's own meta-queries might appear here under high load.

#### Skill: `get_table_hotspots`

-   **Description**: Identifies the top 5 tables with the highest DML (INSERT/UPDATE/DELETE) activity and scan counts, indicating application hotspots.
-   **Usage**: `./run_postgres_check.sh get_table_hotspots`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_table_hotspots",
      "status": "success",
      "data": [
        {"schemaname": "public", "relname": "users", "total_dml": 10000, "total_scans": 50000}
      ]
    }
    ```
-   **Analysis Logic Summary**: Lists the hotspot tables as INFO.

---

## 4. Maintenance & Storage

#### Skill: `get_autovacuum_status`

-   **Description**: Checks for currently running autovacuum or autoanalyze processes.
-   **Usage**: `./run_postgres_check.sh get_autovacuum_status`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_autovacuum_status",
      "status": "success",
      "data": [
        {"pid": 789, "datname": "mydb", "duration": "00:00:30", "query": "autovacuum: VACUUM ..."}
      ]
    }
    ```
-   **Analysis Logic Summary**: Lists active autovacuum workers as INFO.

#### Skill: `get_table_bloat`

-   **Description**: Estimates bloat (wasted space) for the top 10 largest tables based on actual vs. estimated pages, tuple size, and fillfactor.
-   **Usage**: `./run_postgres_check.sh get_table_bloat`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_table_bloat",
      "status": "success",
      "data": [
        {"schemaname": "public", "tablename": "large_table", "total_size_mb": 1000.0, "bloat_percentage": 50.5, "wasted_bytes": 500000000}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports WARNING if bloat > 20% and wasted space > 100MB, otherwise lists as INFO.

#### Skill: `get_index_bloat`

-   **Description**: Estimates bloat (wasted space) for the top 10 largest indexes based on actual vs. estimated pages, tuple size, and fillfactor.
-   **Usage**: `./run_postgres_check.sh get_index_bloat`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_index_bloat",
      "status": "success",
      "data": [
        {"schemaname": "public", "index_name": "idx_large_idx", "total_size_mb": 200.0, "bloat_percentage": 40.0, "wasted_bytes": 80000000}
      ]
    }
    ```
-   **Analysis Logic Summary**: Reports WARNING if bloat > 20% and wasted space > 100MB, otherwise lists as INFO.

#### Skill: `get_top_objects_by_size`

-   **Description**: Lists the top 5 largest tables and indexes by their total disk size.
-   **Usage**: `./run_postgres_check.sh get_top_objects_by_size`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_top_objects_by_size",
      "status": "success",
      "data": [
        {"type": "table", "schemaname": "public", "object_name": "big_table", "size": "10 GB"},
        {"type": "index", "schemaname": "public", "object_name": "big_index", "size": "2 GB"}
      ]
    }
    ```
-   **Analysis Logic Summary**: Lists the top objects as INFO.

#### Skill: `get_database_sizes`

-   **Description**: Reports the size of the top 10 largest non-template databases in the instance.
-   **Usage**: `./run_postgres_check.sh get_database_sizes`
-   **Expected Output (JSON example)**:
    ```json
    {
      "skill": "get_database_sizes",
      "status": "success",
      "data": [
        {"datname": "prod_db", "size": "500 GB"}
      ]
    }
    ```
-   **Analysis Logic Summary**: Lists database sizes as INFO.

---

## 5. Environment Setup

### Virtual Environment

A Python virtual environment is included in the `venv/` directory.

```bash
# Activate venv
cd postgres-daily-check
source venv/bin/activate

# Run health check
python3 scripts/postgres_agent.py

# Deactivate when done
deactivate
```

### Requirements

- `psql` command-line tool (PostgreSQL client)
- No additional Python packages required (uses only standard library)
