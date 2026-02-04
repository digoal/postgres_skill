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

## Available Skills

Each item below represents a callable skill within the `run_postgres_check.sh` script, designed to return structured JSON output.

---

## 1. Core Health & Availability

### Skill: `get_invalid_indexes`

-   **Description**: Checks for any indexes that are in an invalid state. Invalid indexes can cause DML operations to fail or block.
-   **Usage**: `./run_postgres_check.sh get_invalid_indexes`
-   **Expected Output**:
    ```json
    {
      "skill": "get_invalid_indexes",
      "status": "success",
      "data": [
        {"index_name": "idx_my_invalid_idx", "schema_name": "public"}
      ]
    }
    ```
-   **Analysis**: Reports ERROR if any invalid indexes are found.

### Skill: `get_xid_wraparound_risk`

-   **Description**: Monitors the age of transaction IDs (XID) for each database to detect potential transaction ID wraparound issues.
-   **Usage**: `./run_postgres_check.sh get_xid_wraparound_risk`
-   **Expected Output**:
    ```json
    {
      "skill": "get_xid_wraparound_risk",
      "status": "success",
      "data": [
        {"datname": "postgres", "xid_age": 1234567, "percentage_used": 0.05}
      ]
    }
    ```
-   **Analysis**: Reports CRITICAL ERROR if XID age >85%, WARNING if >70%.

### Skill: `get_blocking_locks`

-   **Description**: Identifies active blocking lock situations where one session holds a lock that another is waiting for.
-   **Usage**: `./run_postgres_check.sh get_blocking_locks`
-   **Expected Output**:
    ```json
    {
      "skill": "get_blocking_locks",
      "status": "success",
      "data": [
        {"waiting_pid": 123, "waiting_user": "app_user", "blocking_pid": 456, "blocking_user": "db_admin"}
      ]
    }
    ```
-   **Analysis**: Reports ERROR if any blocking locks are found.

### Skill: `get_deadlock_detection`

-   **Description**: Checks for deadlocks that have occurred since the last stats reset.
-   **Usage**: `./run_postgres_check.sh get_deadlock_detection`
-   **Expected Output**:
    ```json
    {
      "skill": "get_deadlock_detection",
      "status": "success",
      "data": [{"datname": "postgres", "deadlock_count": 0}]
    }
    ```
-   **Analysis**: Reports ERROR if deadlock_count > 0.

### Skill: `get_critical_settings`

-   **Description**: Reviews critical PostgreSQL settings (fsync, synchronous_commit, etc.) for security and performance.
-   **Usage**: `./run_postgres_check.sh get_critical_settings`
-   **Expected Output**:
    ```json
    {
      "skill": "get_critical_settings",
      "status": "success",
      "data": [
        {"name": "fsync", "setting": "on", "short_desc": "..."}
      ]
    }
    ```
-   **Analysis**: Reports CRITICAL ERROR if fsync=off.

---

## 2. Session & Connection Monitoring

### Skill: `get_long_running_queries`

-   **Description**: Finds active queries running longer than threshold (default 5 minutes).
-   **Usage**: `./run_postgres_check.sh get_long_running_queries [threshold_minutes]`
-   **Expected Output**:
    ```json
    {
      "skill": "get_long_running_queries",
      "status": "success",
      "data": [
        {"pid": 123, "usename": "app_user", "duration": "00:06:30", "query": "SELECT ..."}
      ]
    }
    ```
-   **Analysis**: Reports WARNING if any long-running queries found.

### Skill: `get_idle_in_transaction_sessions`

-   **Description**: Detects sessions "idle in transaction" longer than threshold (default 1 minute).
-   **Usage**: `./run_postgres_check.sh get_idle_in_transaction_sessions [threshold_minutes]`
-   **Analysis**: Reports WARNING if any found. These hold locks and prevent VACUUM.

### Skill: `get_long_running_transactions`

-   **Description**: Finds non-idle transactions running longer than threshold (default 1 hour).
-   **Usage**: `./run_postgres_check.sh get_long_running_transactions [threshold_hours]`
-   **Analysis**: Lists transactions ordered by start time.

### Skill: `get_long_running_prepared_transactions`

-   **Description**: Finds prepared transactions (2PC) older than threshold.
-   **Usage**: `./run_postgres_check.sh get_long_running_prepared_transactions [threshold_hours]`
-   **Analysis**: Reports WARNING if any found.

### Skill: `get_connection_usage`

-   **Description**: Reports current connection count vs max_connections.
-   **Usage**: `./run_postgres_check.sh get_connection_usage`
-   **Analysis**: ERROR if >95%, WARNING if >80%.

### Skill: `get_lock_waiters`

-   **Description**: Detailed view of all sessions waiting for locks.
-   **Usage**: `./run_postgres_check.sh get_lock_waiters`
-   **Analysis**: WARNING if >5 lock waiters detected.

### Skill: `get_wait_events`

-   **Description**: Shows current wait events for active sessions (what resources they are waiting on).
-   **Usage**: `./run_postgres_check.sh get_wait_events`
-   **Analysis**: Lists top 10 wait events by occurrence.

---

## 3. Performance & Activity

### Skill: `get_cache_hit_rate`

-   **Description**: Calculates block cache hit rate. Low rate (<99%) indicates memory pressure or bad queries.
-   **Usage**: `./run_postgres_check.sh get_cache_hit_rate`
-   **Analysis**: WARNING if hit rate <99%.

### Skill: `get_rollback_rate`

-   **Description**: Calculates transaction rollback percentage. High rate (>5%) may indicate app issues.
-   **Usage**: `./run_postgres_check.sh get_rollback_rate`
-   **Analysis**: WARNING if rollback rate >5%.

### Skill: `get_top_sql_by_time`

-   **Description**: Top 5 queries by total execution time from pg_stat_statements.
-   **Usage**: `./run_postgres_check.sh get_top_sql_by_time`
-   **Note**: Requires `pg_stat_statements` extension.
-   **Analysis**: Lists queries with total time, avg time, and call count.

### Skill: `get_table_hotspots`

-   **Description**: Tables with highest DML and scan activity.
-   **Usage**: `./run_postgres_check.sh get_table_hotspots`
-   **Analysis**: Lists top 5 tables by total DML operations.

### Skill: `get_bgwriter_stats`

-   **Description**: Background writer statistics and buffer allocation.
-   **Usage**: `./run_postgres_check.sh get_bgwriter_stats`
-   **Analysis**: WARNING if maxwritten_clean > 0.

### Skill: `get_temp_file_usage`

-   **Description**: Shows databases with temporary file usage.
-   **Usage**: `./run_postgres_check.sh get_temp_file_usage`
-   **Analysis**: Lists databases with temp file statistics.

### Skill: `get_io_statistics`

-   **Description**: Reports I/O statistics including temp file usage, block reads/hits, and I/O timing. Useful for identifying I/O bottlenecks and inefficient queries.
-   **Usage**: `./run_postgres_check.sh get_io_statistics`
-   **Analysis**: WARNING if temp_files > 100.

### Skill: `get_analyze_progress`

-   **Description**: Monitors running ANALYZE operations, showing progress, current phase, and scan progress. Useful for identifying long-running statistics collection.
-   **Usage**: `./run_postgres_check.sh get_analyze_progress`
-   **Expected Output**:
    ```json
    {
      "skill": "get_analyze_progress",
      "status": "success",
      "data": [{
        "pid": 12345,
        "datname": "postgres",
        "relname": "users",
        "phase": "acquiring sample rows",
        "sample_blks_total": 1000,
        "sample_blks_scanned": 500,
        "scan_progress_pct": 50.0
      }]
    }
    ```
-   **Analysis**: WARNING if delay_time is high (throttled by vacuum_cost_delay).

### Skill: `get_create_index_progress`

-   **Description**: Monitors CREATE INDEX and REINDEX operations progress, showing phase, blocks, and tuples processed.
-   **Usage**: `./run_postgres_check.sh get_create_index_progress`
-   **Analysis**: Shows current phase and progress. Useful for capacity planning.

### Skill: `get_cluster_progress`

-   **Description**: Monitors CLUSTER and VACUUM FULL operations progress.
-   **Usage**: `./run_postgres_check.sh get_cluster_progress`
-   **Analysis**: Shows phase, tuples scanned/written. WARNING if stuck in 'sorting tuples' phase.

### Skill: `get_wal_statistics`

-   **Description**: Reports WAL activity statistics including records, FPIs, bytes written, and buffer fullness.
-   **Usage**: `./run_postgres_check.sh get_wal_statistics`
-   **Expected Output**:
    ```json
    {
      "skill": "get_wal_statistics",
      "status": "success",
      "data": [{
        "wal_records": 500000,
        "wal_fpi": 10000,
        "wal_bytes": "2 GB",
        "wal_buffers_full": 50
      }]
    }
    ```
-   **Analysis**: WARNING if wal_buffers_full is high (consider increasing wal_buffers).

### Skill: `get_checkpointer_stats`

-   **Description**: Reports checkpointer activity including timed vs requested checkpoints, I/O time, and buffers written.
-   **Usage**: `./run_postgres_check.sh get_checkpointer_stats`
-   **Analysis**: WARNING if requested checkpoints >> timed (tune max_wal_size) or high I/O time.

### Skill: `get_slru_stats`

-   **Description**: Reports SLRU (Simple Least-Recently-Used) cache statistics for internal subsystems like MultiXact, CommitTs.
-   **Usage**: `./run_postgres_check.sh get_slru_stats`
-   **Expected Output**:
    ```json
    {
      "skill": "get_slru_stats",
      "status": "success",
      "data": [
        {"name": "multixact_offset", "blks_hit": 1000, "blks_read": 100, "hit_ratio": 90.9}
      ]
    }
    ```
-   **Analysis**: WARNING if hit ratio < 90% (consider tuning SLRU buffer sizes).

### Skill: `get_database_conflict_stats`

-   **Description**: Reports query cancellations due to conflicts on standby servers (snapshot, lock, tablespace conflicts).
-   **Usage**: `./run_postgres_check.sh get_database_conflict_stats`
-   **Note**: Only applicable on standby servers.
-   **Analysis**: WARNING if any conflicts detected (tune max_standby_streaming_delay).

### Skill: `get_user_function_stats`

-   **Description**: Reports user-defined function performance statistics (calls, total time, self time).
-   **Usage**: `./run_postgres_check.sh get_user_function_stats`
-   **Note**: Requires `track_functions = 'all'` in postgresql.conf.
-   **Analysis**: Lists top time-consuming functions for optimization opportunities.

### Skill: `get_io_statistics_v2`

-   **Description**: Extended I/O statistics from `pg_stat_io` (PostgreSQL 16+), showing detailed read/write/extending statistics by backend type, object type, and context. Provides better visibility into I/O patterns.
-   **Usage**: `./run_postgres_check.sh get_io_statistics_v2`
-   **Analysis**: Reports I/O statistics by backend type and context. Useful for understanding I/O patterns and identifying performance bottlenecks.

---

## 4. Replication & Archiving

### Skill: `get_replication_slots`

-   **Description**: Checks physical and logical replication slots status.
-   **Usage**: `./run_postgres_check.sh get_replication_slots`
-   **Analysis**: ERROR if any inactive slots found.

### Skill: `get_replication_status`

-   **Description**: Checks streaming replication lag and status.
-   **Usage**: `./run_postgres_check.sh get_replication_status`
-   **Analysis**: ERROR if lag >1GB, WARNING if lag >100MB.

### Skill: `get_logical_replication_status`

-   **Description**: Checks logical subscription lag (send/receive delays).
-   **Usage**: `./run_postgres_check.sh get_logical_replication_status`
-   **Analysis**: Lists logical subscriptions with lag seconds.

### Skill: `get_wal_archiver_status`

-   **Description**: WAL archiving status and WAL directory size.
-   **Usage**: `./run_postgres_check.sh get_wal_archiver_status`
-   **Analysis**: ERROR if failed_count > 0.

---

## 5. Maintenance & Storage

### Skill: `get_autovacuum_status`

-   **Description**: Checks currently running autovacuum workers.
-   **Usage**: `./run_postgres_check.sh get_autovacuum_status`
-   **Analysis**: Lists active autovacuum processes.

### Skill: `get_table_bloat`

-   **Description**: Estimates wasted space (bloat) in top 10 largest tables.
-   **Usage**: `./run_postgres_check.sh get_table_bloat`
-   **Analysis**: WARNING if bloat >20% and wasted >100MB.

### Skill: `get_index_bloat`

-   **Description**: Estimates wasted space in top 10 largest indexes.
-   **Usage**: `./run_postgres_check.sh get_index_bloat`
-   **Analysis**: WARNING if bloat >20% and wasted >100MB.

### Skill: `get_top_objects_by_size`

-   **Description**: Top 5 largest tables and indexes by disk size.
-   **Usage**: `./run_postgres_check.sh get_top_objects_by_size`
-   **Note**: Uses relpages to avoid blocking on locked tables.
-   **Analysis**: Lists objects with size.

### Skill: `get_large_unused_indexes`

-   **Description**: Indexes >10MB that have never been scanned (idx_scan=0).
-   **Usage**: `./run_postgres_check.sh get_large_unused_indexes`
-   **Analysis**: WARNING if any found (candidates for removal).

### Skill: `get_stale_statistics`

-   **Description**: Tables with >10% rows modified since last ANALYZE.
-   **Usage**: `./run_postgres_check.sh get_stale_statistics`
-   **Analysis**: WARNING if any found (may cause poor query plans).

### Skill: `get_database_sizes`

-   **Description**: Size of top 10 largest databases.
-   **Usage**: `./run_postgres_check.sh get_database_sizes`
-   **Analysis**: Lists databases with sizes.

### Skill: `get_sequence_exhaustion`

-   **Description**: Sequences approaching max value (>80% used).
-   **Usage**: `./run_postgres_check.sh get_sequence_exhaustion`
-   **Analysis**: WARNING if any sequences near exhaustion.

---

## 6. Freeze & Wraparound Protection

### Skill: `get_freeze_prediction`

-   **Description**: Predicts which tables are approaching XID/MXID freeze thresholds.
-   **Usage**: `./run_postgres_check.sh get_freeze_prediction`
-   **Analysis**: Reports CRITICAL/WARNING based on remaining ages.

---

## 7. Environment Setup

### Requirements

- `psql` command-line tool (PostgreSQL client, version 10+)
- Python 3.6+ (uses only standard library, no additional packages)
- Read access to `pg_stat_statements` (optional, for top SQL queries)

### Configuration

Configure database connection in `assets/db_config.env`:

```bash
export PGHOST="127.0.0.1"
export PGPORT="5432"
export PGUSER="digoal"
export PGPASSWORD="your_password"
export PGDATABASE="postgres"
```

### Usage

```bash
cd postgres-daily-check/scripts

# Run full health check (generates daily_health_report.md)
python3 postgres_agent.py

# Run individual skills
./run_postgres_check.sh get_long_running_queries
./run_postgres_check.sh get_table_bloat
./run_postgres_check.sh get_lock_waiters
```

### Output

The agent generates `daily_health_report.md` with:
- Overall status (OK / WARNING / ERROR)
- Detailed findings for each check
- Actionable recommendations for issues found

---

## Skill Index

| Skill Name | Category | Description |
|------------|----------|-------------|
| get_invalid_indexes | Availability | Check for corrupted indexes |
| get_xid_wraparound_risk | Availability | Monitor transaction ID wraparound |
| get_blocking_locks | Availability | Detect lock contention |
| get_deadlock_detection | Availability | Check for past deadlocks |
| get_critical_settings | Availability | Review critical parameters |
| get_long_running_queries | Session | Find long-running queries |
| get_idle_in_transaction_sessions | Session | Find idle-in-transaction sessions |
| get_long_running_transactions | Session | Find long transactions |
| get_long_running_prepared_transactions | Session | Find stuck 2PC transactions |
| get_connection_usage | Session | Check connection pool usage |
| get_lock_waiters | Session | Detailed lock wait analysis |
| get_wait_events | Session | Current wait event analysis |
| get_cache_hit_rate | Performance | Cache efficiency metric |
| get_rollback_rate | Performance | Transaction rollback ratio |
| get_top_sql_by_time | Performance | Most expensive queries |
| get_table_hotspots | Performance | Most active tables |
| get_bgwriter_stats | Performance | Background writer metrics |
| get_temp_file_usage | Performance | Temporary file usage |
| get_io_statistics | Performance | I/O statistics and timing |
| get_io_statistics_v2 | Performance | Extended I/O statistics (pg_stat_io) |
| get_analyze_progress | Performance | ANALYZE progress monitoring |
| get_create_index_progress | Performance | CREATE INDEX/REINDEX progress |
| get_cluster_progress | Performance | CLUSTER/VACUUM FULL progress |
| get_wal_statistics | Performance | WAL activity statistics |
| get_checkpointer_stats | Performance | Checkpointer activity |
| get_slru_stats | Performance | SLRU cache statistics |
| get_user_function_stats | Performance | UDF performance |
| get_replication_slots | Replication | Replication slot status |
| get_replication_status | Replication | Streaming replica lag |
| get_logical_replication_status | Replication | Logical subscription lag |
| get_wal_archiver_status | Archiving | WAL archiving health |
| get_autovacuum_status | Maintenance | Active vacuum workers |
| get_table_bloat | Maintenance | Table space bloat |
| get_index_bloat | Maintenance | Index space bloat |
| get_top_objects_by_size | Maintenance | Largest objects |
| get_large_unused_indexes | Maintenance | Unused large indexes |
| get_stale_statistics | Maintenance | Outdated table stats |
| get_database_sizes | Storage | Database sizes |
| get_sequence_exhaustion | Storage | Sequence value exhaustion |
| get_freeze_prediction | Storage | Freeze storm prediction |
| get_database_conflict_stats | Standby | Recovery conflicts |
