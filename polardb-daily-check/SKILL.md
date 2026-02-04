# PolarDB Daily Check Agent

This skill guides the agent in conducting a thorough daily health check of a PolarDB for PostgreSQL database instance. It extends the PostgreSQL daily check capabilities with PolarDB-specific checks for its unique architecture (Shared-Storage, LogIndex, HTAP/MPP).

## Purpose

The primary goal of this skill is to empower the agent to proactively monitor the health and performance of PolarDB for PostgreSQL, leveraging its unique features like storage-compute separation, LogIndex, and HTAP capabilities. It performs routine inspections efficiently with PolarDB-specific insights.

## Core Capabilities

The agent performs checks across several key areas:

*   **PolarDB Core Health**: Node type verification, LogIndex status, PFS storage usage
*   **Availability & Health**: Standard PostgreSQL health checks (invalid indexes, XID wraparound, blocking locks)
*   **Performance & Activity**: Active sessions, long-running queries, cache efficiency, MPP parallel query performance
*   **HTAP & MPP**: Parallel query statistics, worker status, coordinator coordination
*   **Storage & I/O**: Shared storage performance, PolarFS usage, buffer pool status
*   **Replication & Consistency**: Primary-readonly node sync status, LogIndex replay lag

## PolarDB-Specific Checks

### LogIndex Architecture

PolarDB uses **LogIndex** to maintain page replay history for read-only nodes, solving the "past page" problem. Key checks include:

- LogIndex replay status and latency
- WAL metadata consistency between primary and read-only nodes
- Lazy and Parallel replay efficiency

### HTAP & MPP Architecture

PolarDB supports HTAP via distributed MPP execution engine:

- **PxScan** and **Shuffle** operators for parallel execution
- Coordinator-Worker coordination for skew elimination
- Serverless elastic scaling of compute nodes
- `polar_enable_px` parameter controls MPP functionality

### Storage-Compute Separation

In this architecture:

- **Storage**: Shared PolarFS, scales independently
- **Compute**: Multiple read-write and read-only nodes, stateless
- **Benefits**: No data replication during scaling, cost-effective read replicas

## Workflow

When activated, this skill executes a predefined sequence of checks:
1. First runs PolarDB-specific checks (node type, LogIndex, PFS)
2. Then runs standard PostgreSQL compatibility checks
3. Generates a comprehensive Markdown report with PolarDB-specific recommendations

## Available Skills

Each item below represents a callable skill, returning structured JSON output.

---

## 1. PolarDB Core Health

### Skill: `get_polar_node_type`

-   **Description**: Checks the current PolarDB node type and role using `polar_node_type()` function.
-   **Usage**: `./run_polardb_check.sh get_polar_node_type`
-   **Expected Output**:
    ```json
    {
      "skill": "get_polar_node_type",
      "status": "success",
      "data": [
        {
          "node_type": "Primary",
          "is_writable": true,
          "polar_version": "2.0"
        }
      ]
    }
    ```
-   **Analysis**: Reports the node role (Primary/ReadOnly) and write capability.

### Skill: `get_logindex_status`

-   **Description**: Monitors LogIndex replay status and lag between primary and read-only nodes.
-   **Usage**: `./run_polardb_check.sh get_logindex_status`
-   **Expected Output**:
    ```json
    {
      "skill": "get_logindex_status",
      "status": "success",
      "data": [
        {
          "node_role": "ReadOnly",
          "replay_lag_mb": 15,
          "replay_lag_seconds": 2,
          "pending_wal_count": 50
        }
      ]
    }
    ```
-   **Analysis**: WARNING if replay lag >100MB or >10 seconds. CRITICAL if lag >1GB.

### Skill: `get_pfs_usage`

-   **Description**: Reports Polar File System (PFS) storage usage using `pfs_du_with_depth()` and `pfs_info()`.
-   **Usage**: `./run_polardb_check.sh get_pfs_usage`
-   **Expected Output**:
    ```json
    {
      "skill": "get_pfs_usage",
      "status": "success",
      "data": [
        {
          "total_size_gb": 500,
          "used_size_gb": 350,
          "used_percentage": 70,
          "file_count": 15000
        }
      ]
    }
    ```
-   **Analysis**: WARNING if usage >80%, CRITICAL if >90%.

### Skill: `get_polar_process_status`

-   **Description**: Detailed process information using `polar_stat_process()` - PID, wait events, I/O stats, CPU, RSS.
-   **Usage**: `./run_polardb_check.sh get_polar_process_status`
-   **Expected Output**:
    ```json
    {
      "skill": "get_polar_process_status",
      "status": "success",
      "data": [
        {
          "pid": 1234,
          "state": "active",
          "wait_event": "ClientRead",
          "cpu_user": 5.2,
          "cpu_system": 1.5,
          "rss_mb": 2048,
          "shared_storage_read_iops": 150,
          "shared_storage_read_throughput_mbps": 50,
          "shared_storage_read_latency_ms": 0.5
        }
      ]
    }
    ```
-   **Analysis**: Reports detailed resource utilization per process.

### Skill: `get_polar_activity`

-   **Description**: Enhanced activity view combining `pg_stat_activity` and `polar_stat_process()`.
-   **Usage**: `./run_polardb_check.sh get_polar_activity`
-   **Expected Output**:
    ```json
    {
      "skill": "get_polar_activity",
      "status": "success",
      "data": [
        {
          "pid": 1234,
          "usename": "app_user",
          "state": "active",
          "query": "SELECT * FROM orders WHERE...",
          "duration": "00:00:05",
          "wait_event": "IO polarfs",
          "shared_io": true
        }
      ]
    }
    ```
-   **Analysis**: Enhanced monitoring with PolarDB-specific wait events.

---

## 2. HTAP & MPP Checks

### Skill: `get_px_workers_status`

-   **Description**: Checks MPP parallel query worker status and configuration.
-   **Usage**: `./run_polardb_check.sh get_px_workers_status`
-   **Expected Output**:
    ```json
    {
      "skill": "get_px_workers_status",
      "status": "success",
      "data": [
        {
          "polar_enable_px": true,
          "polar_px_max_workers_number": 64,
          "polar_px_dop_per_node": 8,
          "active_px_queries": 3,
          "total_px_workers": 24
        }
      ]
    }
    ```
-   **Analysis**: Reports MPP configuration and active parallel queries.

### Skill: `get_px_query_stats`

-   **Description**: Statistics on MPP parallel query execution and performance.
-   **Usage**: `./run_polardb_check.sh get_px_query_stats`
-   **Expected Output**:
    ```json
    {
      "skill": "get_px_query_stats",
      "status": "success",
      "data": [
        {
          "query_type": "PxScan",
          "execution_count": 1500,
          "avg_execution_time_ms": 45,
          "total_rows_scanned": 5000000000
        }
      ]
    }
    ```
-   **Analysis**: Monitors HTAP workload performance.

### Skill: `get_px_nodes`

-   **Description**: Lists nodes participating in MPP execution via `polar_px_nodes`.
-   **Usage**: `./run_polardb_check.sh get_px_nodes`
-   **Expected Output**:
    ```json
    {
      "skill": "get_px_nodes",
      "status": "success",
      "data": [
        {"node_id": 1, "node_name": "primary", "is_coordinator": true},
        {"node_id": 2, "node_name": "readonly1", "is_worker": true},
        {"node_id": 3, "node_name": "readonly2", "is_worker": true}
      ]
    }
    ```
-   **Analysis**: Shows MPP cluster topology.

### Skill: `get_buffer_pool_affinity`

-   **Description**: Checks Buffer Pool affinity settings and effectiveness for MPP operations.
-   **Usage**: `./run_polardb_check.sh get_buffer_pool_affinity`
-   **Expected Output**:
    ```json
    {
      "skill": "get_buffer_pool_affinity",
      "status": "success",
      "data": [
        {
          "buffer_hit_ratio": 99.5,
          "local_buffer_usage": 80,
          "shared_buffer_usage": 75
        }
      ]
    }
    ```
-   **Analysis**: Reports buffer efficiency for shared storage access.

---

## 3. Storage & I/O Performance

### Skill: `get_shared_storage_stats`

-   **Description**: Shared storage I/O performance metrics (IOPS, throughput, latency).
-   **Usage**: `./run_polardb_check.sh get_shared_storage_stats`
-   **Expected Output**:
    ```json
    {
      "skill": "get_shared_storage_stats",
      "status": "success",
      "data": [
        {
          "read_iops": 5000,
          "write_iops": 2000,
          "read_throughput_mbps": 200,
          "write_throughput_mbps": 100,
          "read_latency_ms": 0.3,
          "write_latency_ms": 0.5
        }
      ]
    }
    ```
-   **Analysis**: WARNING if latency >5ms, CRITICAL if >10ms.

### Skill: `get_polar_io_stats`

-   **Description**: Detailed I/O statistics from PolarDB-specific monitoring.
-   **Usage**: `./run_polardb_check.sh get_polar_io_stats`
-   **Expected Output**:
    ```json
    {
      "skill": "get_polar_io_stats",
      "status": "success",
      "data": [
        {
          "polarfs_read_count": 1000000,
          "polarfs_write_count": 500000,
          "polarfs_read_bytes": "50 GB",
          "polarfs_write_bytes": "25 GB",
          "polarfs_iops": 8000,
          "polarfs_throughput_mbps": 350
        }
      ]
    }
    ```
-   **Analysis**: Detailed PolarFS I/O patterns.

### Skill: `get_dirty_page_status`

-   **Description**: Dirty page flush status and coordination with read-only nodes.
-   **Usage**: `./run_polardb_check.sh get_dirty_page_status`
-   **Expected Output**:
    ```json
    {
      "skill": "get_dirty_page_status",
      "status": "success",
      "data": [
        {
          "dirty_pages_count": 5000,
          "dirty_bytes_mb": 200,
          "flush_rate_pages_per_sec": 1000,
          "oldest_modified_age": 120
        }
      ]
    }
    ```
-   **Analysis**: Monitors primary node dirty page flushing coordination.

---

## 4. High Availability & Consistency

### Skill: `get_primary_readonly_sync`

-   **Description**: Synchronization status between primary and read-only nodes.
-   **Usage**: `./run_polardb_check.sh get_primary_readonly_sync`
-   **Expected Output**:
    ```json
    {
      "skill": "get_primary_readonly_sync",
      "status": "success",
      "data": [
        {
          "primary_node": "node_primary",
          "readonly_nodes": [
            {
              "node_name": "node_readonly1",
              "sync_status": "streaming",
              "lag_bytes": 5242880,
              "lag_seconds": 1.5
            }
          ]
        }
      ]
    }
    ```
-   **Analysis**: CRITICAL if any node not in streaming status.

### Skill: `get_online_promote_status`

-   **Description**: Checks readiness for online promotion capability.
-   **Usage**: `./run_polardb_check.sh get_online_promote_status`
-   **Expected Output**:
    ```json
    {
      "skill": "get_online_promote_status",
      "status": "success",
      "data": [
        {
          "promote_ready": true,
          "last_promote_time": "2024-01-15 10:30:00",
          "promote_in_progress": false
        }
      ]
    }
    ```
-   **Analysis**: Reports online promotion readiness.

### Skill: `get_recovery_progress`

-   **Description**: Recovery progress for read-only nodes (if applicable).
-   **Usage**: `./run_polardb_check.sh get_recovery_progress`
-   **Expected Output**:
    ```json
    {
      "skill": "get_recovery_progress",
      "status": "success",
      "data": [
        {
          "node_name": "node_readonly1",
          "received_lsn": "0/ABCDEF00",
          "replayed_lsn": "0/ABCDE800",
          "replay_lag_bytes": 256,
          "is_applying": true
        }
      ]
    }
    ```
-   **Analysis**: Monitors WAL replay progress.

---

## 5. PostgreSQL Compatibility Checks

The following skills are identical to the standard PostgreSQL daily check and work seamlessly with PolarDB.

### Availability & Health

| Skill | Description |
|-------|-------------|
| `get_invalid_indexes` | Check for corrupted indexes |
| `get_xid_wraparound_risk` | Monitor transaction ID wraparound |
| `get_blocking_locks` | Detect lock contention |
| `get_deadlock_detection` | Check for past deadlocks |
| `get_critical_settings` | Review critical parameters |

### Session & Connection Monitoring

| Skill | Description |
|-------|-------------|
| `get_long_running_queries` | Find long-running queries |
| `get_idle_in_transaction_sessions` | Find idle-in-transaction sessions |
| `get_long_running_transactions` | Find long transactions |
| `get_connection_usage` | Check connection pool usage |
| `get_lock_waiters` | Detailed lock wait analysis |
| `get_wait_events` | Current wait event analysis |

### Performance & Activity

| Skill | Description |
|-------|-------------|
| `get_cache_hit_rate` | Cache efficiency metric |
| `get_rollback_rate` | Transaction rollback ratio |
| `get_top_sql_by_time` | Most expensive queries |
| `get_table_hotspots` | Most active tables |
| `get_bgwriter_stats` | Background writer metrics |
| `get_wal_statistics` | WAL activity statistics |

### Replication & Archiving

| Skill | Description |
|-------|-------------|
| `get_replication_slots` | Replication slot status |
| `get_replication_status` | Streaming replica lag |
| `get_wal_archiver_status` | WAL archiving health |

### Maintenance & Storage

| Skill | Description |
|-------|-------------|
| `get_autovacuum_status` | Active vacuum workers |
| `get_table_bloat` | Table space bloat |
| `get_index_bloat` | Index space bloat |
| `get_top_objects_by_size` | Largest objects |
| `get_stale_statistics` | Outdated table stats |
| `get_database_sizes` | Database sizes |

### Freeze & Wraparound Protection

| Skill | Description |
|-------|-------------|
| `get_freeze_prediction` | Predict freeze thresholds |

---

## Environment Setup

### Requirements

- `psql` command-line tool (PostgreSQL client, version 10+)
- Python 3.6+ (uses only standard library, no additional packages)
- `polar_monitor` extension installed and available
- Read access to PolarDB-specific functions

### Configuration

Configure database connection in `assets/db_config.env`:

```bash
export PGHOST="127.0.0.1"
export PGPORT="5432"
export PGUSER="digoal"
export PGPASSWORD="your_password"
export PGDATABASE="postgres"
```

### Required Extensions

Ensure the following extensions are available:

- `polar_monitor` - Core PolarDB monitoring functions
- `pg_stat_statements` - SQL performance statistics (optional)
- `pg_buffercache` - Buffer pool analysis (optional)

### Installation Check

Verify PolarDB extensions are installed:

```sql
SELECT * FROM pg_extension WHERE extname LIKE 'polar%';
SELECT * FROM pg_extension WHERE extname = 'pg_stat_statements';
```

---

## Usage

### Run Full Health Check

```bash
cd polardb-daily-check/scripts

# Run full health check (generates polar_daily_health_report.md)
python3 polardb_agent.py

# Or run via the bash wrapper
./run_polardb_check.sh full_check
```

### Run Individual PolarDB-Specific Checks

```bash
./run_polardb_check.sh get_polar_node_type
./run_polardb_check.sh get_logindex_status
./run_polardb_check.sh get_pfs_usage
./run_polardb_check.sh get_px_workers_status
./run_polardb_check.sh get_shared_storage_stats
```

### Run Standard PostgreSQL Compatibility Checks

```bash
./run_polardb_check.sh get_long_running_queries
./run_polardb_check.sh get_table_bloat
./run_polardb_check.sh get_replication_status
./run_polardb_check.sh get_cache_hit_rate
```

---

## Output

### PolarDB Daily Health Report

The agent generates `polar_daily_health_report.md` with:

1. **PolarDB-Specific Status**
   - Node type and role
   - LogIndex replay lag
   - PFS storage usage
   - MPP/HTAP status

2. **Overall Health Status**
   - OK / WARNING / CRITICAL

3. **Detailed Findings**
   - PolarDB-specific issues
   - Standard PostgreSQL issues
   - Performance recommendations

4. **Actionable Recommendations**
   - LogIndex optimization
   - MPP tuning suggestions
   - Storage capacity planning
   - High availability readiness

### Sample Report Structure

```markdown
# PolarDB Daily Health Report
Generated: 2024-01-15 10:00:00

## PolarDB Status
- Node Type: Primary (Writable)
- LogIndex Lag: 5MB (2s) ✅ OK
- PFS Usage: 70% ✅ OK
- MPP Enabled: true ✅ OK

## Overall Status: ✅ HEALTHY

### Critical Issues
None

### Warnings
- 3 long-running queries detected
- Table 'orders' has 15% bloat

### Recommendations
1. Consider running VACUUM FULL on 'orders' table
2. Review slow queries in pg_stat_statements
```

---

## PolarDB-Specific Recommendations

### LogIndex Optimization

- Monitor replay lag continuously
- If lag increases, check:
  - Network bandwidth between compute and storage
  - Storage I/O performance
  - Read-only node workload

### MPP Tuning

- Adjust `polar_px_max_workers_number` based on workload
- Use `ALTER TABLE ... SET(px_workers=...)` for large tables only
- Monitor `polar_px_dop_per_node` for parallel degree

### Storage Capacity

- Plan for 70% PFS usage threshold
- Consider storage tiering for cold data
- Monitor I/O latency for performance degradation

### High Availability

- Test online promotion periodically
- Monitor sync lag between nodes
- Keep `polar_enable_px` consistent across nodes

---

## Skill Index

### PolarDB Core Health

| Skill | Category | Description |
|-------|----------|-------------|
| `get_polar_node_type` | Core | Node type verification |
| `get_logindex_status` | Core | LogIndex replay status |
| `get_pfs_usage` | Core | PolarFS storage usage |
| `get_polar_process_status` | Core | Detailed process metrics |
| `get_polar_activity` | Core | Enhanced activity monitor |

### HTAP & MPP

| Skill | Category | Description |
|-------|----------|-------------|
| `get_px_workers_status` | HTAP | MPP worker configuration |
| `get_px_query_stats` | HTAP | Parallel query statistics |
| `get_px_nodes` | HTAP | MPP cluster topology |
| `get_buffer_pool_affinity` | HTAP | Buffer efficiency |

### Storage & I/O

| Skill | Category | Description |
|-------|----------|-------------|
| `get_shared_storage_stats` | I/O | Shared storage performance |
| `get_polar_io_stats` | I/O | PolarFS detailed I/O |
| `get_dirty_page_status` | I/O | Dirty page coordination |

### High Availability

| Skill | Category | Description |
|-------|----------|-------------|
| `get_primary_readonly_sync` | HA | Primary-readonly sync |
| `get_online_promote_status` | HA | Promotion readiness |
| `get_recovery_progress` | HA | Recovery progress |

### PostgreSQL Compatibility

| Skill | Category | Description |
|-------|----------|-------------|
| *All standard PostgreSQL checks* | Various | 40+ compatibility skills |

---

## Notes

This skill is designed for **PolarDB for PostgreSQL** and requires the `polar_monitor` extension. Standard PostgreSQL checks work on both PolarDB and regular PostgreSQL instances.

For more information about PolarDB architecture:
- [PolarDB Architecture Overview](polar-doc/docs/zh/theory/arch-overview.md)
- [PolarDB HTAP Architecture](polar-doc/docs/zh/theory/arch-htap.md)
- [PolarDB Operations Guide](polar-doc/docs/zh/operation/)

Base directory for this skill: file:///Users/digoal/.config/opencode/skills/polardb-daily-check
Relative paths in this skill (e.g., scripts/, assets/) are relative to this base directory.
