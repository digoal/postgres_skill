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

*   **Availability & Health**: Verifying database responsiveness and detecting critical errors like invalid indexes, XID/MultiXactId wraparound risks, and deadlock occurrences.
*   **Performance & Activity**: Monitoring active sessions, long-running queries, cache efficiency, transaction rollback rates, identifying application hotspots, and tracking temporary file usage.
*   **Security**: Checking connection encryption status (SSL/GSSAPI) to ensure data transmission security.
*   **Replication & Archiving**: Ensuring data consistency and recoverability by checking replication lag, slot status, and WAL archiving health.
*   **Maintenance & Storage**: Analyzing disk usage, identifying bloat in tables and indexes, checking autovacuum activity, and monitoring sequence exhaustion.

## Workflow

When activated, this skill executes a predefined sequence of checks. Each check involves calling a specialized script (`run_postgres_check.sh`) which executes specific SQL queries against the target PostgreSQL database. The results are then analyzed by the agent, and a comprehensive Markdown report is generated.

## Warning Strategy Guidelines

The default warning thresholds are designed for general-purpose use. You should adjust them based on your specific workload characteristics:

### OLTP Systems (Low Latency Required)
- **Long-running queries**: Set threshold to 1 minute or less
- **Cache hit rate**: Target >99%, warning at <97%
- **Connection usage**: More aggressive thresholds (WARNING at >70%, ERROR at >90%)

### Analytical/Reporting Systems
- **Long-running queries**: Higher thresholds acceptable (10-30 minutes)
- **Cache hit rate**: Lower hit rates expected due to large scans (warning at <90%)
- **Temp file usage**: Higher tolerance for temporary files

### General Recommendations
- **bgwriter maxwritten_clean**: Monitor trends over time rather than single values
- **Checkpointer stats**: Focus on average write/sync times per checkpoint rather than totals
- **XID/MultiXactId wraparound**: Always maintain conservative thresholds regardless of workload

All thresholds in `postgres_agent.py` can be customized by modifying the analysis logic.

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

### Skill: `get_multixid_wraparound_risk`

-   **Description**: Monitors the age of MultiXactId (multi-transaction IDs) for each database. Similar to XID wraparound, MultiXactId wraparound can also lead to data loss and requires monitoring. **Note**: `age(datminmxid) = 2147483647` (INT_MAX) indicates the MultiXactId is frozen or invalid, which is normal and not a risk.
-   **Usage**: `./run_postgres_check.sh get_multixid_wraparound_risk`
-   **Expected Output**:
    ```json
    {
      "skill": "get_multixid_wraparound_risk",
      "status": "success",
      "data": [
        {
          "datname": "postgres",
          "mxid_age": 1234567,
          "datminmxid": 12345,
          "freeze_max_age": 400000000,
          "status": "OK",
          "remaining_to_autovacuum": 398765433
        }
      ]
    }
    ```
-   **Analysis**: 
    -   **FROZEN/INVALID**: `datminmxid = 0` or `age = INT_MAX` - MultiXactIds are frozen, no risk ✅
    -   **OK**: More than 30M MultiXactIds remaining before forced autovacuum ✅
    -   **WARNING**: Less than 40M remaining - Getting close to threshold 🟠
    -   **CRITICAL**: Less than 3M remaining - Approaching wraparound ❌
    -   **FORCE_AUTOVACUUM**: Age >= `autovacuum_multixact_freeze_max_age` - Autovacuum will be forced 🟠
-   **Official Doc**: [PostgreSQL Routine Maintenance - Preventing Transaction ID Wraparound Failures](https://www.postgresql.org/docs/current/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND)

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

### Skill: `get_connection_security_status`

-   **Description**: Checks SSL/GSSAPI encryption status for active connections. Security audit to ensure connections use proper encryption.
-   **Usage**: `./run_postgres_check.sh get_connection_security_status`
-   **Expected Output**:
    ```json
    {
      "skill": "get_connection_security_status",
      "status": "success",
      "data": [
        {"datname": "postgres", "usename": "app_user", "ssl_enabled": true, "ssl_version": "TLSv1.3", "connection_type": "SSL"}
      ]
    }
    ```
-   **Analysis**: Reports WARNING if unencrypted remote TCP connections found. SSL or GSSAPI encryption recommended for production.
-   **Official Doc**: [PostgreSQL SSL Support](https://www.postgresql.org/docs/current/ssl-tcp.html), [pg_stat_ssl](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SSL), [pg_stat_gssapi](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-GSSAPI)

---

## 2. Session & Connection Monitoring

### Skill: `get_long_running_queries`

-   **Description**: Finds active queries running longer than threshold (default 5 minutes for general use, 1 minute recommended for OLTP systems).
-   **Usage**: `./run_postgres_check.sh get_long_running_queries [threshold_minutes]`
-   **Parameters**: `threshold_minutes` - Threshold in minutes (default: 5)
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
-   **Analysis**: Reports WARNING if any long-running queries found. For OLTP systems with strict response time requirements, consider setting threshold to 1 minute.

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

-   **Description**: Calculates block cache hit rate. Low rate indicates memory pressure or bad queries. Threshold should be adjusted based on workload characteristics.
-   **Usage**: `./run_postgres_check.sh get_cache_hit_rate`
-   **Analysis**: WARNING if hit rate <95%. For some workloads, 90% may be acceptable. OLTP systems should target >99%, while analytical workloads may have lower hit rates due to large table scans.
-   **Note**: The 99% threshold may be too strict for certain workloads. Adjust based on your performance baseline.

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
-   **Analysis**: WARNING if maxwritten_clean remains high over time (persistent high values indicate bgwriter tuning issues). Occasional >0 values are normal. Consider monitoring the trend rather than single values.
-   **Note**: maxwritten_clean indicates the number of times the background writer stopped a cleaning scan because it had written too many buffers. While this can indicate insufficient bgwriter capacity, occasional occurrences are normal and not necessarily a concern.

### Skill: `get_temp_file_usage`

-   **Description**: Shows databases with temporary file usage.
-   **Usage**: `./run_postgres_check.sh get_temp_file_usage`
-   **Analysis**: Lists databases with temp file statistics.

### Skill: `get_total_temp_bytes`

-   **Description**: Shows databases with total temporary file bytes exceeding threshold. More accurate than file count for disk space impact assessment.
-   **Usage**: `./run_postgres_check.sh get_total_temp_bytes [threshold_gb]`
-   **Parameters**: `threshold_gb` - Minimum temp bytes in GB to report (default: 1GB)
-   **Expected Output**:
    ```json
    {
      "skill": "get_total_temp_bytes",
      "status": "success",
      "data": [
        {"datname": "postgres", "temp_files": 150, "temp_bytes": 2147483648, "temp_bytes_gb": 2.0}
      ]
    }
    ```
-   **Analysis**: Reports WARNING if total temp space >10GB. High temp usage indicates insufficient work_mem or inefficient queries.
-   **Official Doc**: [pg_stat_database](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE)

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

### Skill: `get_slru_stats`

-   **Description**: Reports SLRU (Simple Least-Recently-Used) cache statistics for internal subsystems like MultiXact, CommitTs, Subtransaction, and Transaction managers. Low hit ratios may indicate undersized SLRU buffers.
-   **Usage**: `./run_postgres_check.sh get_slru_stats`
-   **Expected Output**:
    ```json
    {
      "skill": "get_slru_stats",
      "status": "success",
      "data": [
        {"name": "transaction", "blks_hit": 84086814, "blks_read": 1768614, "hit_ratio": 97.94}
      ]
    }
    ```
-   **Analysis**: WARNING if hit ratio < 90% and blks_read > 1000. Low hit ratios may require tuning SLRU buffer sizes via postgresql.conf.
-   **Official Doc**: [pg_stat_slru](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-SLRU)

### Skill: `get_io_statistics_v2`

-   **Description**: Extended I/O statistics from `pg_stat_io` (PostgreSQL 16+), showing detailed read/write/extending statistics by backend type, object type, and context. Provides better visibility into I/O patterns than traditional pg_stat_database.
-   **Usage**: `./run_postgres_check.sh get_io_statistics_v2`
-   **Note**: Only available on PostgreSQL 16 and later. Falls back gracefully on older versions.
-   **Expected Output**:
    ```json
    {
      "skill": "get_io_statistics_v2",
      "status": "success",
      "data": [
        {
          "backend_type": "client backend",
          "object": "relation",
          "context": "normal",
          "reads": 1000,
          "read_bytes": 8192000,
          "writes": 500,
          "write_bytes": 4096000,
          "hits": 50000
        }
      ]
    }
    ```
-   **Analysis**: Reports I/O statistics by backend type and context. Useful for understanding I/O patterns, identifying which backend types are causing I/O, and pinpointing performance bottlenecks. Compare reads vs hits to assess cache efficiency per backend type.
-   **Official Doc**: [pg_stat_io](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-IO)

### Skill: `get_user_function_stats`

-   **Description**: Reports user-defined function performance statistics including call counts, total execution time, and self time. Helps identify slow or frequently called functions that may need optimization.
-   **Usage**: `./run_postgres_check.sh get_user_function_stats`
-   **Note**: Requires `track_functions = 'all'` or `'pl'` in postgresql.conf. If disabled, no data will be available.
-   **Expected Output**:
    ```json
    {
      "skill": "get_user_function_stats",
      "status": "success",
      "data": [
        {
          "funcid": 16384,
          "schemaname": "public",
          "funcname": "my_function",
          "calls": 1000,
          "total_time": 5000.0,
          "self_time": 3000.0,
          "avg_time_ms": 5.0
        }
      ]
    }
    ```
-   **Analysis**: Lists top time-consuming functions ordered by total_time. Functions with high total_time or high avg_time_ms are candidates for optimization. Consider function refactoring, better indexing, or query optimization.
-   **Official Doc**: [pg_stat_user_functions](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-USER-FUNCTIONS)

### Skill: `get_database_conflict_stats`

-   **Description**: Reports query cancellations due to conflicts on standby servers. Conflicts occur when standby queries conflict with WAL replay operations (e.g., dropping a table being queried, tablespace changes, or snapshot conflicts).
-   **Usage**: `./run_postgres_check.sh get_database_conflict_stats`
-   **Note**: Only applicable on standby/replica servers. Will return empty on primary servers.
-   **Expected Output**:
    ```json
    {
      "skill": "get_database_conflict_stats",
      "status": "success",
      "data": [
        {
          "datname": "postgres",
          "confl_tablespace": 0,
          "confl_lock": 0,
          "confl_snapshot": 5,
          "confl_bufferpin": 0,
          "confl_deadlock": 0,
          "confl_active_logicalslot": 0
        }
      ]
    }
    ```
-   **Analysis**: WARNING if any conflicts detected. High snapshot conflicts indicate standby queries running too long while WAL is being replayed. Consider tuning `max_standby_streaming_delay` or `hot_standby_feedback`. Lock conflicts may require shorter queries on standby.
-   **Official Doc**: [pg_stat_database_conflicts](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-DATABASE-CONFLICTS)

### Skill: `get_checkpointer_stats`

-   **Description**: Reports checkpointer activity including timed vs requested checkpoints, I/O time, and buffers written.
-   **Usage**: `./run_postgres_check.sh get_checkpointer_stats`
-   **Analysis**: WARNING if requested checkpoints >> timed (tune max_wal_size) or high I/O time.

### Skill: `get_checkpointer_write_sync_time`

-   **Description**: Detailed analysis of checkpointer write and sync times to identify I/O bottlenecks. Calculates average times per checkpoint.
-   **Usage**: `./run_postgres_check.sh get_checkpointer_write_sync_time`
-   **Expected Output**:
    ```json
    {
      "skill": "get_checkpointer_write_sync_time",
      "status": "success",
      "data": [
        {
          "num_timed": 100,
          "num_requested": 50,
          "write_time_ms": 5000.0,
          "sync_time_ms": 2000.0,
          "avg_write_time_per_checkpoint_ms": 33.33,
          "avg_sync_time_per_checkpoint_ms": 13.33,
          "checkpointer_status": "OK"
        }
      ]
    }
    ```
-   **Analysis**: Reports WARNING if avg write/sync time per checkpoint >5000ms, or if requested checkpoints significantly exceed timed checkpoints. Helps identify storage I/O bottlenecks.
-   **Official Doc**: [pg_stat_checkpointer](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-CHECKPOINTER)

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
| get_multixid_wraparound_risk | Availability | Monitor MultiXactId wraparound |
| get_blocking_locks | Availability | Detect lock contention |
| get_deadlock_detection | Availability | Check for past deadlocks |
| get_critical_settings | Availability | Review critical parameters |
| get_connection_security_status | Security | Check SSL/GSSAPI encryption status |
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
| get_total_temp_bytes | Performance | Total temp file bytes by database |
| get_io_statistics | Performance | I/O statistics and timing |
| get_io_statistics_v2 | Performance | Extended I/O statistics (pg_stat_io) |
| get_analyze_progress | Performance | ANALYZE progress monitoring |
| get_create_index_progress | Performance | CREATE INDEX/REINDEX progress |
| get_cluster_progress | Performance | CLUSTER/VACUUM FULL progress |
| get_wal_statistics | Performance | WAL activity statistics |
| get_checkpointer_stats | Performance | Checkpointer activity |
| get_checkpointer_write_sync_time | Performance | Checkpointer I/O time analysis |
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
