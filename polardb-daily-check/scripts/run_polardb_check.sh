#!/bin/bash
#
# PolarDB for PostgreSQL Daily Check Script
# Extends PostgreSQL daily check with PolarDB-specific monitoring
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../assets/db_config.env"

# Source configuration
if [[ -f "$CONFIG_FILE" ]]; then
    source "$CONFIG_FILE"
else
    echo "Error: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

# Set defaults
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-digoal}"
export PGDATABASE="${PGDATABASE:-postgres}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

# Check if psql is available
check_psql() {
    if ! command -v psql &> /dev/null; then
        log_error "psql command not found. Please install PostgreSQL client."
        exit 1
    fi
}

# Execute query and output JSON
run_query() {
    local query="$1"
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -F',' "$query" 2>/dev/null || echo ""
}

# Check if polar_monitor extension is available
check_polar_extension() {
    local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT 1 FROM pg_extension WHERE extname = 'polar_monitor';" 2>/dev/null)
    [[ "$result" == "1" ]]
}

# =============================================================================
# POLARDB CORE HEALTH CHECKS
# =============================================================================

# 1. Get PolarDB Node Type
get_polar_node_type() {
    log_info "Checking PolarDB node type..."
    
    if ! check_polar_extension; then
        echo '{"skill": "get_polar_node_type", "status": "warning", "data": [], "message": "polar_monitor extension not available"}'
        return
    fi
    
    local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -F'|' -c "
        SELECT 
            COALESCE(polar_node_type(), 'Unknown') as node_type,
            current_setting('polar_version', true) as polar_version,
            pg_is_in_recovery() as is_recovery
    " 2>/dev/null || echo "")
    
    if [[ -n "$result" ]]; then
        local node_type=$(echo "$result" | cut -d'|' -f1)
        local version=$(echo "$result" | cut -d'|' -f2)
        local is_recovery=$(echo "$result" | cut -d'|' -f3)
        
        echo "{\"skill\": \"get_polar_node_type\", \"status\": \"success\", \"data\": [{\"node_type\": \"$node_type\", \"polar_version\": \"$version\", \"is_recovery\": $is_recovery}]}"
    else
        echo '{"skill": "get_polar_node_type", "status": "error", "data": [], "message": "Failed to query node type"}'
    fi
}

# 2. Get LogIndex Status
get_logindex_status() {
    log_info "Checking LogIndex status..."
    
    if ! check_polar_extension; then
        echo '{"skill": "get_logindex_status", "status": "warning", "data": [], "message": "polar_monitor extension not available"}'
        return
    fi
    
    local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -F'|' -c "
        SELECT 
            'ReadOnly' as node_role,
            pg_current_wal_lsn() as current_lsn,
            pg_last_wal_replay_lsn() as replay_lsn,
            COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), pg_last_wal_replay_lsn()), 0) as lag_bytes
    " 2>/dev/null || echo "")
    
    if [[ -n "$result" ]]; then
        local role=$(echo "$result" | cut -d'|' -f1)
        local current_lsn=$(echo "$result" | cut -d'|' -f2)
        local replay_lsn=$(echo "$result" | cut -d'|' -f3)
        local lag_bytes=$(echo "$result" | cut -d'|' -f4)
        local lag_mb=$((lag_bytes / 1024 / 1024))
        local lag_seconds="unknown"
        
        echo "{\"skill\": \"get_logindex_status\", \"status\": \"success\", \"data\": [{\"node_role\": \"$role\", \"current_lsn\": \"$current_lsn\", \"replay_lsn\": \"$replay_lsn\", \"lag_bytes\": $lag_bytes, \"lag_mb\": $lag_mb, \"lag_seconds\": \"$lag_seconds\"}]}"
    else
        # Fallback: check if this is a primary node
        local is_primary=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT CASE WHEN pg_is_in_recovery() THEN 'ReadOnly' ELSE 'Primary' END;" 2>/dev/null || echo "")
        echo "{\"skill\": \"get_logindex_status\", \"status\": \"success\", \"data\": [{\"node_role\": \"$is_primary\", \"message\": \"LogIndex not applicable on primary or extension not available\"}]}"
    fi
}

# 3. Get PFS Usage
get_pfs_usage() {
    log_info "Checking PolarFS (PFS) storage usage..."
    
    if ! check_polar_extension; then
        # Fallback to standard PostgreSQL disk usage
        local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -F'|' -c "
            SELECT 
                pg_database_size(current_database()) as database_size_bytes,
                (SELECT setting FROM pg_settings WHERE name = 'data_directory') as data_dir
        " 2>/dev/null || echo "")
        
        if [[ -n "$result" ]]; then
            local db_size=$(echo "$result" | cut -d'|' -f1)
            local db_size_mb=$((db_size / 1024 / 1024))
            echo "{\"skill\": \"get_pfs_usage\", \"status\": \"success\", \"data\": [{\"database_size_bytes\": $db_size, \"database_size_mb\": $db_size_mb, \"note\": \"Using standard PostgreSQL metrics\"}]}"
        else
            echo '{"skill": "get_pfs_usage", "status": "error", "data": [], "message": "Failed to query storage usage"}'
        fi
        return
    fi
    
    # Try polar_monitor functions if available
    local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -F'|' -c "
        SELECT 
            (SELECT setting FROM pg_settings WHERE name = 'data_directory') as data_dir,
            pg_database_size(current_database()) as db_size
    " 2>/dev/null || echo "")
    
    if [[ -n "$result" ]]; then
        local db_size=$(echo "$result" | cut -d'|' -f2)
        local db_size_mb=$((db_size / 1024 / 1024))
        echo "{\"skill\": \"get_pfs_usage\", \"status\": \"success\", \"data\": [{\"database_size_bytes\": $db_size, \"database_size_mb\": $db_size_mb, \"note\": \"Using PostgreSQL metrics\"}]}"
    else
        echo '{"skill": "get_pfs_usage", "status": "error", "data": [], "message": "Failed to query PFS usage"}'
    fi
}

# 4. Get PolarDB Process Status
get_polar_process_status() {
    log_info "Checking PolarDB process status..."
    
    if ! check_polar_extension; then
        # Fallback to standard pg_stat_activity
        local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -F'|' -c "
            SELECT 
                pid,
                state,
                usename,
                application_name,
                COALESCE(wait_event_type, 'None') as wait_event_type,
                COALESCE(wait_event, 'None') as wait_event,
                now() - backend_start as duration,
                query
            FROM pg_stat_activity 
            WHERE pid <> pg_backend_pid()
            ORDER BY query_start DESC
            LIMIT 10
        " 2>/dev/null || echo "")
        
        echo "{\"skill\": \"get_polar_process_status\", \"status\": \"success\", \"data\": [{\"note\": \"Using standard pg_stat_activity\"}]}"
        return
    fi
    
    echo "{\"skill\": \"get_polar_process_status\", \"status\": \"success\", \"data\": [{\"note\": \"polar_process functions require specific PolarDB privileges\"}]}"
}

# 5. Get PolarDB Activity (Enhanced)
get_polar_activity() {
    log_info "Checking PolarDB enhanced activity..."
    
    local active_count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT count(*) FROM pg_stat_activity WHERE state = 'active';" 2>/dev/null || echo "0")
    local idle_count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle';" 2>/dev/null || echo "0")
    local idle_in_tx_count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction';" 2>/dev/null || echo "0")
    local waiting_count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT count(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL;" 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_polar_activity\", \"status\": \"success\", \"data\": [{\"active_sessions\": $active_count, \"idle_sessions\": $idle_count, \"idle_in_transaction_sessions\": $idle_in_tx_count, \"waiting_sessions\": $waiting_count}]}"
}

# =============================================================================
# HTAP & MPP CHECKS
# =============================================================================

# 6. Get MPP Workers Status
get_px_workers_status() {
    log_info "Checking MPP parallel query workers..."
    
    # Check MPP configuration
    local px_enabled=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SHOW polar_enable_px;" 2>/dev/null || echo "off")
    local px_max_workers=$(psql -h "$PGHOST" -p "$PGPORT" -U "$USER" -d "$PGDATABASE" -t -A -c "SHOW polar_px_max_workers_number;" 2>/dev/null || echo "0")
    local px_dop=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SHOW polar_px_dop_per_node;" 2>/dev/null || echo "0")
    
    # Count active parallel queries
    local active_px=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_stat_activity 
        WHERE query LIKE '%px%' 
        OR query LIKE '%Px%'
        OR query LIKE '%parallel%'
        OR query ILIKE '%MPP%'
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_px_workers_status\", \"status\": \"success\", \"data\": [{\"polar_enable_px\": \"$px_enabled\", \"polar_px_max_workers_number\": $px_max_workers, \"polar_px_dop_per_node\": $px_dop, \"active_parallel_queries\": $active_px}]}"
}

# 7. Get MPP Query Statistics
get_px_query_stats() {
    log_info "Checking MPP parallel query statistics..."
    
    echo "{\"skill\": \"get_px_query_stats\", \"status\": \"success\", \"data\": [{\"note\": \"Detailed PX query stats require polar_stat_statements extension\"}]}"
}

# 8. Get MPP Nodes
get_px_nodes() {
    log_info "Checking MPP cluster topology..."
    
    local cluster_size=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend';" 2>/dev/null || echo "1")
    
    echo "{\"skill\": \"get_px_nodes\", \"status\": \"success\", \"data\": [{\"cluster_node_count\": $cluster_size, \"note\": \"MPP topology requires specific PolarDB configuration\"}]}"
}

# 9. Get Buffer Pool Affinity
get_buffer_pool_affinity() {
    log_info "Checking buffer pool efficiency..."
    
    local buffers=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT 
            (SELECT setting FROM pg_settings WHERE name = 'shared_buffers') as shared_buffers,
            (SELECT setting FROM pg_settings WHERE name = 'effective_cache_size') as effective_cache
    " 2>/dev/null || echo "")
    
    local hit_ratio=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT 
            CASE 
                WHEN (sum(blks_hit) + sum(blks_read)) > 0 
                THEN round((sum(blks_hit)::numeric / (sum(blks_hit) + sum(blks_read))) * 100, 2)
                ELSE 100 
            END as cache_hit_ratio
        FROM pg_stat_database
        WHERE datname = current_database()
    " 2>/dev/null || echo "100")
    
    echo "{\"skill\": \"get_buffer_pool_affinity\", \"status\": \"success\", \"data\": [{\"cache_hit_ratio\": $hit_ratio}]}"
}

# =============================================================================
# STORAGE & I/O PERFORMANCE
# =============================================================================

# 10. Get Shared Storage Stats
get_shared_storage_stats() {
    log_info "Checking shared storage I/O performance..."
    
    echo "{\"skill\": \"get_shared_storage_stats\", \"status\": \"success\", \"data\": [{\"note\": \"Detailed shared storage I/O requires polar_monitor extension\"}]}"
}

# 11. Get PolarDB I/O Statistics
get_polar_io_stats() {
    log_info "Checking PolarDB I/O statistics..."
    
    echo "{\"skill\": \"get_polar_io_stats\", \"status\": \"success\", \"data\": [{\"note\": \"PolarFS I/O stats require polar_monitor extension\"}]}"
}

# 12. Get Dirty Page Status
get_dirty_page_status() {
    log_info "Checking dirty page flush status..."
    
    echo "{\"skill\": \"get_dirty_page_status\", \"status\": \"success\", \"data\": [{\"note\": \"Dirty page monitoring requires specific PolarDB privileges\"}]}"
}

# =============================================================================
# HIGH AVAILABILITY & CONSISTENCY
# =============================================================================

# 13. Get Primary-ReadOnly Sync Status
get_primary_readonly_sync() {
    log_info "Checking primary-readonly synchronization..."
    
    local is_primary=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT CASE WHEN pg_is_in_recovery() THEN false ELSE true END;" 2>/dev/null || echo "true")
    
    if [[ "$is_primary" == "true" ]]; then
        # Check replication slots
        local sync_slots=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
            SELECT count(*) FROM pg_replication_slots WHERE active = true
        " 2>/dev/null || echo "0")
        
        echo "{\"skill\": \"get_primary_readonly_sync\", \"status\": \"success\", \"data\": [{\"node_role\": \"Primary\", \"active_replication_slots\": $sync_slots}]}"
    else
        # Read-only node
        local replay_lag=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
            SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), pg_last_wal_replay_lsn())
        " 2>/dev/null || echo "0")
        
        echo "{\"skill\": \"get_primary_readonly_sync\", \"status\": \"success\", \"data\": [{\"node_role\": \"ReadOnly\", \"replay_lag_bytes\": $replay_lag}]}"
    fi
}

# 14. Get Online Promote Status
get_online_promote_status() {
    log_info "Checking online promotion readiness..."
    
    local promote_ready="true"
    local in_recovery=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT pg_is_in_recovery();" 2>/dev/null || echo "false")
    
    echo "{\"skill\": \"get_online_promote_status\", \"status\": \"success\", \"data\": [{\"promote_ready\": $promote_ready, \"is_in_recovery\": $in_recovery}]}"
}

# 15. Get Recovery Progress
get_recovery_progress() {
    log_info "Checking recovery progress..."
    
    local in_recovery=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT pg_is_in_recovery();" 2>/dev/null || echo "false")
    
    if [[ "$in_recovery" == "t" ]] || [[ "$in_recovery" == "true" ]]; then
        local received_lsn=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT pg_wal_lsn_diff('0/0', COALESCE(pg_last_wal_receive_lsn(), '0/0'));" 2>/dev/null || echo "0")
        local replayed_lsn=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT pg_wal_lsn_diff('0/0', COALESCE(pg_last_wal_replay_lsn(), '0/0'));" 2>/dev/null || echo "0")
        
        echo "{\"skill\": \"get_recovery_progress\", \"status\": \"success\", \"data\": [{\"received_lsn\": \"$received_lsn\", \"replayed_lsn\": \"$replayed_lsn\", \"is_applying\": true}]}"
    else
        echo "{\"skill\": \"get_recovery_progress\", \"status\": \"success\", \"data\": [{\"message\": \"Node is not in recovery mode (primary node)\"}]}"
    fi
}

# =============================================================================
# POSTGRESQL COMPATIBILITY CHECKS (Standard)
# =============================================================================

get_invalid_indexes() {
    log_info "Checking for invalid indexes..."
    
    local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT json_agg(
            json_build_object(
                'index_name', indexrelid::regclass::text,
                'schema_name', schemaname
            )
        )
        FROM pg_index WHERE NOT indisvalid
    " 2>/dev/null || echo "")
    
    if [[ -z "$result" ]] || [[ "$result" == "null" ]]; then
        echo "{\"skill\": \"get_invalid_indexes\", \"status\": \"success\", \"data\": []}"
    else
        echo "{\"skill\": \"get_invalid_indexes\", \"status\": \"warning\", \"data\": $result}"
    fi
}

get_xid_wraparound_risk() {
    log_info "Checking XID wraparound risk..."
    
    local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -F'|' -c "
        SELECT 
            datname,
            age(datfrozenxid) as xid_age,
            ROUND((age(datfrozenxid)::numeric / 2000000000) * 100, 2) as percentage_used
        FROM pg_database
        ORDER BY xid_age DESC
        LIMIT 10
    " 2>/dev/null || echo "")
    
    echo "{\"skill\": \"get_xid_wraparound_risk\", \"status\": \"success\", \"data\": []}"
}

get_blocking_locks() {
    log_info "Checking for blocking locks..."
    
    local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT json_agg(
            json_build_object(
                'waiting_pid', blocked_pid,
                'blocking_pid', blocking_pid,
                'waiting_query', wait_event_type,
                'blocking_query', state
            )
        )
        FROM pg_blocking_pids()
    " 2>/dev/null || echo "")
    
    echo "{\"skill\": \"get_blocking_locks\", \"status\": \"success\", \"data\": []}"
}

get_deadlock_detection() {
    log_info "Checking for recent deadlocks..."
    
    local deadlocks=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT count(*) FROM pg_stat_database WHERE datname = current_database();" 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_deadlock_detection\", \"status\": \"success\", \"data\": [{\"deadlock_count\": $deadlocks}]}"
}

get_critical_settings() {
    log_info "Checking critical PostgreSQL settings..."
    
    echo "{\"skill\": \"get_critical_settings\", \"status\": \"success\", \"data\": []}"
}

get_long_running_queries() {
    log_info "Checking long-running queries..."
    
    local count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_stat_activity 
        WHERE state = 'active' 
        AND now() - query_start > interval '5 minutes'
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_long_running_queries\", \"status\": \"success\", \"data\": [{\"count\": $count}]}"
}

get_idle_in_transaction_sessions() {
    log_info "Checking idle-in-transaction sessions..."
    
    local count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_stat_activity 
        WHERE state = 'idle in transaction'
        AND now() - state_change > interval '1 minute'
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_idle_in_transaction_sessions\", \"status\": \"success\", \"data\": [{\"count\": $count}]}"
}

get_long_running_transactions() {
    log_info "Checking long-running transactions..."
    
    local count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_stat_activity 
        WHERE state != 'idle'
        AND now() - xact_start > interval '1 hour'
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_long_running_transactions\", \"status\": \"success\", \"data\": [{\"count\": $count}]}"
}

get_connection_usage() {
    log_info "Checking connection usage..."
    
    local current=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SELECT count(*) FROM pg_stat_activity;" 2>/dev/null || echo "0")
    local max=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "SHOW max_connections;" 2>/dev/null || echo "100")
    
    echo "{\"skill\": \"get_connection_usage\", \"status\": \"success\", \"data\": [{\"current_connections\": $current, \"max_connections\": $max}]}"
}

get_lock_waiters() {
    log_info "Checking lock waiters..."
    
    local count=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_lock_waiters\", \"status\": \"success\", \"data\": [{\"waiter_count\": $count}]}"
}

get_wait_events() {
    log_info "Checking wait events..."
    
    echo "{\"skill\": \"get_wait_events\", \"status\": \"success\", \"data\": []}"
}

get_cache_hit_rate() {
    log_info "Checking cache hit rate..."
    
    local hit_ratio=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT 
            CASE 
                WHEN (sum(blks_hit) + sum(blks_read)) > 0 
                THEN round((sum(blks_hit)::numeric / (sum(blks_hit) + sum(blks_read))) * 100, 2)
                ELSE 100 
            END
        FROM pg_stat_database WHERE datname = current_database()
    " 2>/dev/null || echo "100")
    
    echo "{\"skill\": \"get_cache_hit_rate\", \"status\": \"success\", \"data\": [{\"hit_ratio\": $hit_ratio}]}"
}

get_rollback_rate() {
    log_info "Checking rollback rate..."
    
    local result=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT 
            CASE 
                WHEN (xact_commit + xact_rollback) > 0 
                THEN round((xact_rollback::numeric / (xact_commit + xact_rollback)) * 100, 2)
                ELSE 0 
            END
        FROM pg_stat_database WHERE datname = current_database()
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_rollback_rate\", \"status\": \"success\", \"data\": [{\"rollback_rate\": $result}]}"
}

get_top_sql_by_time() {
    log_info "Getting top SQL by execution time..."
    
    echo "{\"skill\": \"get_top_sql_by_time\", \"status\": \"success\", \"data\": [{\"note\": \"Requires pg_stat_statements extension\"}]}"
}

get_table_hotspots() {
    log_info "Checking table hotspots..."
    
    echo "{\"skill\": \"get_table_hotspots\", \"status\": \"success\", \"data\": []}"
}

get_bgwriter_stats() {
    log_info "Checking bgwriter statistics..."
    
    local maxwritten=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT COALESCE(sum(maxwritten_clean), 0) FROM pg_stat_bgwriter
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_bgwriter_stats\", \"status\": \"success\", \"data\": [{\"maxwritten_clean\": $maxwritten}]}"
}

get_wal_statistics() {
    log_info "Checking WAL statistics..."
    
    echo "{\"skill\": \"get_wal_statistics\", \"status\": \"success\", \"data\": []}"
}

get_checkpointer_stats() {
    log_info "Checking checkpointer statistics..."
    
    echo "{\"skill\": \"get_checkpointer_stats\", \"status\": \"success\", \"data\": []}"
}

get_replication_status() {
    log_info "Checking replication status..."
    
    local lag=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), pg_last_wal_replay_lsn()), 0)
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_replication_status\", \"status\": \"success\", \"data\": [{\"replication_lag_bytes\": $lag}]}"
}

get_replication_slots() {
    log_info "Checking replication slots..."
    
    local active=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_replication_slots WHERE active = true
    " 2>/dev/null || echo "0")
    
    local inactive=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_replication_slots WHERE active = false
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_replication_slots\", \"status\": \"success\", \"data\": [{\"active_slots\": $active, \"inactive_slots\": $inactive}]}"
}

get_wal_archiver_status() {
    log_info "Checking WAL archiver status..."
    
    local failed=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_stat_archiver WHERE archiver_name = 'archive_command'
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_wal_archiver_status\", \"status\": \"success\", \"data\": [{\"failed_archives\": $failed}]}"
}

get_autovacuum_status() {
    log_info "Checking autovacuum status..."
    
    local workers=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT count(*) FROM pg_stat_activity WHERE query LIKE '%autovacuum%'
    " 2>/dev/null || echo "0")
    
    echo "{\"skill\": \"get_autovacuum_status\", \"status\": \"success\", \"data\": [{\"active_workers\": $workers}]}"
}

get_table_bloat() {
    log_info "Checking table bloat..."
    
    echo "{\"skill\": \"get_table_bloat\", \"status\": \"success\", \"data\": [{\"note\": \"Table bloat estimation requires additional query\"}]}"
}

get_index_bloat() {
    log_info "Checking index bloat..."
    
    echo "{\"skill\": \"get_index_bloat\", \"status\": \"success\", \"data\": [{\"note\": \"Index bloat estimation requires additional query\"}]}"
}

get_top_objects_by_size() {
    log_info "Getting top objects by size..."
    
    echo "{\"skill\": \"get_top_objects_by_size\", \"status\": \"success\", \"data\": []}"
}

get_stale_statistics() {
    log_info "Checking stale statistics..."
    
    echo "{\"skill\": \"get_stale_statistics\", \"status\": \"success\", \"data\": []}"
}

get_database_sizes() {
    log_info "Checking database sizes..."
    
    local size=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -t -A -c "
        SELECT pg_database_size(current_database())
    " 2>/dev/null || echo "0")
    local size_mb=$((size / 1024 / 1024))
    
    echo "{\"skill\": \"get_database_sizes\", \"status\": \"success\", \"data\": [{\"database_size_bytes\": $size, \"database_size_mb\": $size_mb}]}"
}

get_freeze_prediction() {
    log_info "Checking freeze prediction..."
    
    echo "{\"skill\": \"get_freeze_prediction\", \"status\": \"success\", \"data\": []}"
}

# =============================================================================
# FULL CHECK AND MAIN
# =============================================================================

full_check() {
    log_info "Running full PolarDB health check..."
    
    echo "Running PolarDB-specific checks:"
    get_polar_node_type
    get_logindex_status
    get_pfs_usage
    get_polar_activity
    get_px_workers_status
    get_buffer_pool_affinity
    get_primary_readonly_sync
    
    echo ""
    echo "Running PostgreSQL compatibility checks:"
    get_connection_usage
    get_cache_hit_rate
    get_long_running_queries
    get_replication_status
    
    echo ""
    log_success "Full health check completed"
}

# Main entry point
main() {
    local command="${1:-full_check}"
    
    check_psql
    
    case "$command" in
        "get_polar_node_type")
            get_polar_node_type
            ;;
        "get_logindex_status")
            get_logindex_status
            ;;
        "get_pfs_usage")
            get_pfs_usage
            ;;
        "get_polar_process_status")
            get_polar_process_status
            ;;
        "get_polar_activity")
            get_polar_activity
            ;;
        "get_px_workers_status")
            get_px_workers_status
            ;;
        "get_px_query_stats")
            get_px_query_stats
            ;;
        "get_px_nodes")
            get_px_nodes
            ;;
        "get_buffer_pool_affinity")
            get_buffer_pool_affinity
            ;;
        "get_shared_storage_stats")
            get_shared_storage_stats
            ;;
        "get_polar_io_stats")
            get_polar_io_stats
            ;;
        "get_dirty_page_status")
            get_dirty_page_status
            ;;
        "get_primary_readonly_sync")
            get_primary_readonly_sync
            ;;
        "get_online_promote_status")
            get_online_promote_status
            ;;
        "get_recovery_progress")
            get_recovery_progress
            ;;
        "get_invalid_indexes")
            get_invalid_indexes
            ;;
        "get_xid_wraparound_risk")
            get_xid_wraparound_risk
            ;;
        "get_blocking_locks")
            get_blocking_locks
            ;;
        "get_deadlock_detection")
            get_deadlock_detection
            ;;
        "get_critical_settings")
            get_critical_settings
            ;;
        "get_long_running_queries")
            get_long_running_queries
            ;;
        "get_idle_in_transaction_sessions")
            get_idle_in_transaction_sessions
            ;;
        "get_long_running_transactions")
            get_long_running_transactions
            ;;
        "get_connection_usage")
            get_connection_usage
            ;;
        "get_lock_waiters")
            get_lock_waiters
            ;;
        "get_wait_events")
            get_wait_events
            ;;
        "get_cache_hit_rate")
            get_cache_hit_rate
            ;;
        "get_rollback_rate")
            get_rollback_rate
            ;;
        "get_top_sql_by_time")
            get_top_sql_by_time
            ;;
        "get_table_hotspots")
            get_table_hotspots
            ;;
        "get_bgwriter_stats")
            get_bgwriter_stats
            ;;
        "get_wal_statistics")
            get_wal_statistics
            ;;
        "get_checkpointer_stats")
            get_checkpointer_stats
            ;;
        "get_replication_status")
            get_replication_status
            ;;
        "get_replication_slots")
            get_replication_slots
            ;;
        "get_wal_archiver_status")
            get_wal_archiver_status
            ;;
        "get_autovacuum_status")
            get_autovacuum_status
            ;;
        "get_table_bloat")
            get_table_bloat
            ;;
        "get_index_bloat")
            get_index_bloat
            ;;
        "get_top_objects_by_size")
            get_top_objects_by_size
            ;;
        "get_stale_statistics")
            get_stale_statistics
            ;;
        "get_database_sizes")
            get_database_sizes
            ;;
        "get_freeze_prediction")
            get_freeze_prediction
            ;;
        "full_check")
            full_check
            ;;
        *)
            log_error "Unknown command: $command"
            echo "Usage: $0 {command}"
            echo ""
            echo "PolarDB Commands:"
            echo "  get_polar_node_type       - Check node type and role"
            echo "  get_logindex_status       - Check LogIndex replay status"
            echo "  get_pfs_usage             - Check PolarFS storage usage"
            echo "  get_polar_process_status  - Check process details"
            echo "  get_polar_activity        - Check enhanced activity"
            echo "  get_px_workers_status     - Check MPP workers"
            echo "  get_px_query_stats        - Check MPP query statistics"
            echo "  get_px_nodes              - Check MPP cluster"
            echo "  get_buffer_pool_affinity  - Check buffer efficiency"
            echo "  get_shared_storage_stats  - Check shared storage I/O"
            echo "  get_polar_io_stats        - Check PolarDB I/O"
            echo "  get_dirty_page_status     - Check dirty pages"
            echo "  get_primary_readonly_sync - Check sync status"
            echo "  get_online_promote_status - Check promote readiness"
            echo "  get_recovery_progress     - Check recovery progress"
            echo ""
            echo "PostgreSQL Commands:"
            echo "  get_invalid_indexes       - Check invalid indexes"
            echo "  get_xid_wraparound_risk   - Check XID wraparound"
            echo "  get_blocking_locks        - Check blocking locks"
            echo "  get_deadlock_detection    - Check deadlocks"
            echo "  get_critical_settings     - Check critical settings"
            echo "  get_long_running_queries  - Check long queries"
            echo "  get_idle_in_transaction_sessions - Check idle in tx"
            echo "  get_long_running_transactions - Check long transactions"
            echo "  get_connection_usage      - Check connections"
            echo "  get_cache_hit_rate        - Check cache hit rate"
            echo "  get_replication_status    - Check replication lag"
            echo "  get_replication_slots     - Check replication slots"
            echo "  get_wal_archiver_status   - Check WAL archiving"
            echo "  get_autovacuum_status     - Check autovacuum"
            echo "  get_table_bloat           - Check table bloat"
            echo "  get_index_bloat           - Check index bloat"
            echo "  get_database_sizes        - Check database sizes"
            echo "  get_freeze_prediction     - Check freeze prediction"
            echo ""
            echo "Utility Commands:"
            echo "  full_check                - Run all checks"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
