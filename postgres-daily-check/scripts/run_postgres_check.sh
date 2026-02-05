#!/bin/bash

# =================================================================
# PostgreSQL Skill Executor for AI Agent
# =================================================================
# This script acts as the low-level tool for the AI agent.
# It takes a skill name as an argument, executes the corresponding
# SQL query, and formats the output as a JSON object.
#
# Usage:
# ./run_postgres_check.sh <skill_name> [parameters...]
# e.g., ./run_postgres_check.sh get_long_running_queries 5
# =================================================================

# --- Configuration ---
# Source the database configuration file if it exists.
if [ -f "$(dirname "$0")/../assets/db_config.env" ]; then
    source "$(dirname "$0")/../assets/db_config.env"
fi

# --- Helper Functions ---
# Function to execute a query and return JSON.
# It handles psql errors and empty outputs gracefully.
function execute_sql_as_json() {
    local query="$1"
    local psql_vars="$2" # New parameter for psql -v options
    local psql_output
    local psql_exit_code

    # Run psql and capture its stdout and exit code.
    # We capture stderr as part of output here, to include potential psql errors.
    psql_output=$(psql -X -q -A -t --host="$PGHOST" --port="$PGPORT" --username="$PGUSER" --dbname="$PGDATABASE" $psql_vars -c "$query" 2>&1)
    psql_exit_code=$?

    if [ $psql_exit_code -ne 0 ]; then
        # psql command failed (non-zero exit code). Return an error JSON.
        echo "{\"skill\": \"unknown_skill\", \"status\": \"fail\", \"data\": \"PSQL command failed with exit code $psql_exit_code. Error: $(echo \"$psql_output\" | head -n 1)\"}"
        return 1 # Indicate shell function failure
    fi

    # If psql succeeded (exit code 0) but returned no output, this is an unexpected state
    # as our queries are designed to always produce JSON. As a fallback, report this.
    if [ -z "$psql_output" ]; then
        echo "{\"skill\": \"unknown_skill\", \"status\": \"fail\", \"data\": \"SQL query executed successfully but returned empty output from psql.\"}"
        return 1 # Indicate shell function failure
    fi

    # Otherwise, assume psql_output contains the expected JSON.
    echo "$psql_output"
    return 0
}

# --- Skill Definitions (All functions must be defined before the main case statement) ---

function get_long_running_queries() {
    local threshold=${1:-5} # Default to 5 minutes if not provided. 
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_long_running_queries',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            pid,
            age(clock_timestamp(), query_start) AS duration,
            usename,
            datname,
            state,
            query
        FROM
            pg_stat_activity
        WHERE
            state = 'active'
            AND backend_type = 'client backend'
            AND query_start < now() - interval '$threshold minutes'
        ORDER BY
            duration DESC
    ) t;
EOF
)
    execute_sql_as_json "$query" ""
}

function get_idle_in_transaction_sessions() {
    local threshold=${1:-1} # Default to 1 minute
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_idle_in_transaction_sessions',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            pid,
            age(clock_timestamp(), xact_start) AS transaction_duration,
            usename,
            datname,
            state
        FROM
            pg_stat_activity
        WHERE
            state = 'idle in transaction'
            AND backend_type = 'client backend'
            AND xact_start < now() - interval '$threshold minutes'
        ORDER BY
            transaction_duration DESC
    ) t;
EOF
)
    execute_sql_as_json "$query" ""
}

function get_blocking_locks() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_blocking_locks',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            waiting_activity.pid AS waiting_pid,
            waiting_activity.usename AS waiting_user,
            waiting_activity.query AS waiting_query,
            age(clock_timestamp(), waiting_activity.query_start) as waiting_duration,
            blocking_activity.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocking_activity.query AS blocking_query,
            age(clock_timestamp(), blocking_activity.query_start) as blocking_duration
        FROM
            pg_stat_activity AS waiting_activity
        JOIN
            pg_locks AS waiting ON waiting_activity.pid = waiting.pid AND NOT waiting.granted
        JOIN
            pg_stat_activity AS blocking_activity ON blocking_activity.pid = ANY(pg_blocking_pids(waiting.pid))
        WHERE
            waiting_activity.backend_type = 'client backend'
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_cache_hit_rate() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_cache_hit_rate',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            datname,
            blks_read,
            blks_hit,
            CASE WHEN (blks_hit + blks_read) = 0 THEN 0
                 ELSE ROUND((blks_hit::numeric * 100) / (blks_hit + blks_read), 2)
            END AS hit_rate_percentage
        FROM
            pg_stat_database
        WHERE (blks_hit + blks_read) > 0 AND datname = current_database()
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_replication_status() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_replication_status',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            application_name,
            client_addr,
            state,
            sync_state,
            sync_priority,
            pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) AS sent_lag_bytes,
            pg_wal_lsn_diff(flush_lsn, replay_lsn) AS replay_lag_bytes,
            write_lag::text AS write_lag_time,
            flush_lag::text AS flush_lag_time,
            replay_lag::text AS replay_lag_time
        FROM
            pg_stat_replication
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_database_sizes() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_database_sizes',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            datname,
            pg_size_pretty(pg_database_size(datname)) AS size
        FROM
            pg_database
        WHERE
            datistemplate = false
        ORDER BY
            pg_database_size(datname) DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_connection_usage() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_connection_usage',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend') AS used_connections,
            (SELECT setting FROM pg_settings WHERE name = 'max_connections')::int AS max_connections
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_xid_wraparound_risk() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_xid_wraparound_risk',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            d.datname,
            age(d.datfrozenxid) AS xid_age,
            TRUNC(100 * (age(d.datfrozenxid)::numeric / 2147483647), 2)::numeric AS percentage_used
        FROM
            pg_database d
        WHERE
            d.datallowconn = true
        ORDER BY
            xid_age DESC
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_invalid_indexes() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_invalid_indexes',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            c.relname as index_name,
            n.nspname as schema_name
        FROM
            pg_class c
        JOIN
            pg_index i ON c.oid = i.indexrelid
        JOIN
            pg_namespace n ON n.oid = c.relnamespace
        WHERE
            i.indisvalid = false
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_rollback_rate() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_rollback_rate',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            datname,
            xact_commit,
            xact_rollback,
            CASE WHEN (xact_commit + xact_rollback) = 0 THEN 0
                 ELSE TRUNC((xact_rollback::numeric / (xact_commit + xact_rollback)) * 100, 2)::numeric
            END AS rollback_percentage
        FROM
            pg_stat_database
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_replication_slots() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_replication_slots',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            slot_name,
            plugin,
            slot_type,
            database,
            active,
            pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as restart_lsn_lag_bytes
        FROM
            pg_replication_slots
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_autovacuum_status() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_autovacuum_status',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            pid,
            datname,
            age(clock_timestamp(), query_start) as duration,
            query
        FROM
            pg_stat_activity
        WHERE
            query ILIKE 'autovacuum: %'
        ORDER BY
            duration DESC
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_top_sql_by_time() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_top_sql_by_time',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            (total_exec_time / 1000 / 60)::numeric(10, 2) as total_minutes,
            (total_exec_time / calls)::numeric(10, 2) as avg_ms,
            calls,
            query
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT 5
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_top_objects_by_size() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_top_objects_by_size',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        (SELECT
            'table' as type,
            s.schemaname,
            s.relname as object_name,
            pg_size_pretty(c.relpages * current_setting('block_size')::bigint) as size
        FROM pg_stat_user_tables s
        JOIN pg_class c ON c.oid = s.relid
        ORDER BY c.relpages DESC
        LIMIT 5)
        UNION ALL
        (SELECT
            'index' as type,
            s.schemaname,
            s.indexrelname as object_name,
            pg_size_pretty(c.relpages * current_setting('block_size')::bigint) as size
        FROM pg_stat_user_indexes s
        JOIN pg_class c ON c.oid = s.indexrelid
        ORDER BY c.relpages DESC
        LIMIT 5)
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_table_hotspots() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_table_hotspots',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            schemaname,
            relname,
            (n_tup_ins + n_tup_upd + n_tup_del) as total_dml,
            (seq_scan + idx_scan) as total_scans,
            n_dead_tup
        FROM pg_stat_user_tables
        ORDER BY (n_tup_ins + n_tup_upd + n_tup_del) DESC, (seq_scan + idx_scan) DESC
        LIMIT 5
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_wal_archiver_status() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_wal_archiver_status',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            archived_count,
            last_archived_wal,
            last_archived_time,
            failed_count,
            last_failed_wal,
            last_failed_time,
            stats_reset,
            (
                SELECT pg_size_pretty(
                    COALESCE(
                        SUM(size)::bigint,
                        (SELECT pg_database_size(current_database()) * 0.1)
                    )
                )
                FROM pg_ls_waldir()
            ) AS wal_directory_size
        FROM pg_stat_archiver
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_large_unused_indexes() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_large_unused_indexes',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            s.schemaname,
            s.relname as table_name,
            s.indexrelname as index_name,
            pg_size_pretty(pg_relation_size(s.indexrelid)) as index_size
        FROM pg_stat_user_indexes s
        JOIN pg_index i ON s.indexrelid = i.indexrelid
        WHERE s.idx_scan = 0
          AND i.indisunique IS FALSE
          AND i.indisprimary IS FALSE
          AND pg_relation_size(s.indexrelid) > 10 * 1024 * 1024 -- > 10MB
        ORDER BY pg_relation_size(s.indexrelid) DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_long_running_prepared_transactions() {
    local threshold_hours=${1:-1} # Default to 1 hour
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_long_running_prepared_transactions',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            gid,
            owner,
            database,
            prepared,
            now() - prepared AS duration,
            transaction
        FROM pg_prepared_xacts
        WHERE now() - prepared > ('${threshold_hours} hour')::interval
        ORDER BY prepared ASC
    ) t;
EOF
)
    execute_sql_as_json "$query" ""
}

function get_logical_replication_status() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_logical_replication_status',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            subname,
            subid,
            EXTRACT(EPOCH FROM (now() - last_msg_send_time)) AS send_lag_sec,
            EXTRACT(EPOCH FROM (now() - last_msg_receipt_time)) AS receive_lag_sec
        FROM pg_stat_subscription
        WHERE subid IS NOT NULL
        ORDER BY subname
    ) t;
EOF
)
    execute_sql_as_json "$query" ""
}


function get_table_bloat() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_table_bloat',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        WITH tbl_stats AS (
            SELECT
                c.oid,
                c.reltuples,
                c.relpages,
                n.nspname,
                c.relname,
                COALESCE((
                    SELECT AVG(s.avg_width)
                    FROM pg_stats s
                    WHERE s.schemaname = n.nspname
                      AND s.tablename = c.relname
                ), 32) AS avg_row_width
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND c.relpages > 100
              AND c.reltuples > 0
        ),
        bloat_calc AS (
            SELECT
                oid,
                nspname,
                relname,
                reltuples,
                relpages,
                pg_relation_size(oid) AS total_bytes,
                (relpages - (reltuples * ((avg_row_width + 24) / current_setting('block_size')::numeric))) * current_setting('block_size')::numeric AS wasted_bytes,
                (100.0 * (relpages - (reltuples * ((avg_row_width + 24) / current_setting('block_size')::numeric))) / relpages) AS bloat_percentage
            FROM tbl_stats
        )
        SELECT
            bc.nspname AS schemaname,
            bc.relname AS tablename,
            bc.reltuples AS live_tuples,
            bc.relpages AS actual_pages,
            bc.total_bytes,
            (bc.total_bytes / (1024^2))::numeric(10,2) AS total_size_mb,
            bc.bloat_percentage::numeric(5,2),
            bc.wasted_bytes::bigint
        FROM bloat_calc bc
        WHERE bc.bloat_percentage > 10
        ORDER BY wasted_bytes DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_index_bloat() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_index_bloat',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        WITH idx_stats AS (
            SELECT
                c.oid,
                c.reltuples,
                c.relpages,
                n.nspname,
                c.relname,
                i.indrelid,
                COALESCE(SUM(s.avg_width), 8) AS total_index_width
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_index i ON i.indexrelid = c.oid
            JOIN pg_class c_tbl ON c_tbl.oid = i.indrelid
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            LEFT JOIN pg_stats s ON s.schemaname = n.nspname AND s.tablename = c_tbl.relname AND s.attname = a.attname
            WHERE c.relkind = 'i'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
              AND c.relpages > 100
              AND c.reltuples > 0
            GROUP BY c.oid, c.reltuples, c.relpages, n.nspname, c.relname, i.indrelid
        ),
        bloat_calc AS (
            SELECT
                oid, nspname, relname, reltuples, relpages,
                pg_relation_size(oid) AS total_bytes,
                (relpages - (reltuples * ((total_index_width + 16) / current_setting('block_size')::numeric))) * current_setting('block_size')::numeric AS wasted_bytes,
                (100.0 * (relpages - (reltuples * ((total_index_width + 16) / current_setting('block_size')::numeric))) / relpages) AS bloat_percentage
            FROM idx_stats
        )
        SELECT
            bc.nspname AS schemaname,
            bc.relname AS index_name,
            bc.reltuples AS live_tuples,
            bc.relpages AS actual_pages,
            bc.total_bytes,
            (bc.total_bytes / (1024^2))::numeric(10,2) AS total_size_mb,
            bc.bloat_percentage::numeric(5,2),
            bc.wasted_bytes::bigint
        FROM bloat_calc bc
        WHERE bc.bloat_percentage > 10
        ORDER BY wasted_bytes DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_freeze_prediction() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_freeze_prediction',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        WITH table_mxid_status AS (
            SELECT
                c.oid,
                c.relname,
                n.nspname,
                c.relminmxid,
                age(c.relfrozenxid) AS xid_age,
                age(c.relminmxid) AS mxid_age,
                CASE
                    WHEN c.relminmxid = 0 THEN TRUE
                    WHEN age(c.relminmxid) = 2147483647 THEN TRUE
                    ELSE FALSE
                END AS is_mxid_invalid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'm')
            AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        )
        SELECT
            oid::regclass AS table_name,
            nspname AS schemaname,
            pg_size_pretty(pg_total_relation_size(oid)) AS total_size,
            (current_setting('autovacuum_freeze_max_age')::int - xid_age) AS xid_remain_ages,
            CASE
                WHEN is_mxid_invalid THEN NULL
                ELSE (current_setting('autovacuum_multixact_freeze_max_age')::int - mxid_age)
            END AS mxid_remain_ages,
            CASE
                WHEN (current_setting('autovacuum_freeze_max_age')::int - xid_age) < 0 THEN 'XID_OVERDUE'
                WHEN is_mxid_invalid THEN 'MXID_NA'
                WHEN (current_setting('autovacuum_multixact_freeze_max_age')::int - mxid_age) < 0 THEN 'MXID_OVERDUE'
                WHEN LEAST(
                    (current_setting('autovacuum_freeze_max_age')::int - xid_age),
                    (current_setting('autovacuum_multixact_freeze_max_age')::int - mxid_age)
                ) < (current_setting('autovacuum_freeze_max_age')::int * 0.10) THEN 'CRITICAL'
                WHEN LEAST(
                    (current_setting('autovacuum_freeze_max_age')::int - xid_age),
                    (current_setting('autovacuum_multixact_freeze_max_age')::int - mxid_age)
                ) < (current_setting('autovacuum_freeze_max_age')::int * 0.25) THEN 'WARNING'
                ELSE 'OK'
            END AS freeze_status
        FROM table_mxid_status
        WHERE
            (current_setting('autovacuum_freeze_max_age')::int - xid_age) < (current_setting('autovacuum_freeze_max_age')::int * 0.25)
            OR
            (NOT is_mxid_invalid AND (current_setting('autovacuum_multixact_freeze_max_age')::int - mxid_age) < (current_setting('autovacuum_multixact_freeze_max_age')::int * 0.25))
        ORDER BY freeze_status, LEAST(
            (current_setting('autovacuum_freeze_max_age')::int - xid_age),
            CASE WHEN is_mxid_invalid THEN 2147483647 ELSE (current_setting('autovacuum_multixact_freeze_max_age')::int - mxid_age) END
        )
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_wait_events() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_wait_events',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            wait_event_type,
            wait_event,
            count(*) AS occurrences
        FROM pg_stat_activity
        WHERE state = 'active' AND wait_event IS NOT NULL AND backend_type = 'client backend'
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_critical_settings() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_critical_settings',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            name,
            setting,
            short_desc
        FROM pg_settings
        WHERE name IN (
            'fsync',
            'synchronous_commit',
            'log_min_duration_statement',
            'log_lock_waits',
            'track_io_timing'
        )
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_stale_statistics() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_stale_statistics',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            schemaname,
            relname,
            n_live_tup,
            last_autoanalyze,
            (n_mod_since_analyze::numeric / GREATEST(n_live_tup, 1) * 100)::numeric(10,2) AS modified_percent
        FROM pg_stat_user_tables
        WHERE
            n_live_tup > 1000 AND
            (n_mod_since_analyze::numeric / GREATEST(n_live_tup, 1)) > 0.10
        ORDER BY modified_percent DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_sequence_exhaustion() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_sequence_exhaustion',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            n.nspname AS schemaname,
            c.relname AS sequence_name,
            pg_sequence_last_value(c.oid) AS last_val,
            s.seqmax AS max_val,
            (pg_sequence_last_value(c.oid)::numeric * 100) / s.seqmax::numeric AS percentage_used
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_sequence s ON s.seqrelid = c.oid
        WHERE c.relkind = 'S'
          AND NOT s.seqcycle
          AND (pg_sequence_last_value(c.oid)::numeric / s.seqmax::numeric) > 0.8
        ORDER BY percentage_used DESC
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_temp_file_usage() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_temp_file_usage',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            datname,
            temp_files,
            temp_bytes,
            pg_size_pretty(temp_bytes) AS temp_bytes_pretty,
            temp_files::float / NULLIF((
                SELECT SUM(temp_files) FROM pg_stat_database WHERE temp_files > 0
            ), 0) AS temp_files_ratio
        FROM pg_stat_database
        WHERE temp_files > 0
        ORDER BY temp_bytes DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_io_statistics() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_io_statistics',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            datname,
            temp_files,
            temp_bytes,
            pg_size_pretty(temp_bytes) as temp_bytes_pretty,
            blks_read,
            blks_hit,
            blks_read + blks_hit as total_blks,
            blk_read_time,
            blk_write_time
        FROM pg_stat_database
        WHERE datname = current_database()
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_analyze_progress() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_analyze_progress',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            p.pid,
            d.datname,
            c.relname,
            p.phase,
            p.sample_blks_total,
            p.sample_blks_scanned,
            ROUND((p.sample_blks_scanned::numeric / NULLIF(p.sample_blks_total, 0) * 100), 2) AS scan_progress_pct,
            p.child_tables_total,
            p.child_tables_done,
            p.delay_time
        FROM pg_stat_progress_analyze p
        JOIN pg_database d ON p.datid = d.oid
        LEFT JOIN pg_class c ON p.relid = c.oid
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_create_index_progress() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_create_index_progress',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            p.pid,
            d.datname,
            c.relname AS table_name,
            i.relname AS index_name,
            p.command,
            p.phase,
            p.blocks_total,
            p.blocks_done,
            p.tuples_total,
            p.tuples_done,
            p.partitions_total,
            p.partitions_done
        FROM pg_stat_progress_create_index p
        JOIN pg_database d ON p.datid = d.oid
        LEFT JOIN pg_class c ON p.relid = c.oid
        LEFT JOIN pg_class i ON p.index_relid = i.oid
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_cluster_progress() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_cluster_progress',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            p.pid,
            d.datname,
            c.relname,
            p.command,
            p.phase,
            p.heap_blks_total,
            p.heap_tuples_scanned,
            p.heap_tuples_written,
            p.cluster_index_relid
        FROM pg_stat_progress_cluster p
        JOIN pg_database d ON p.datid = d.oid
        LEFT JOIN pg_class c ON p.relid = c.oid
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_wal_statistics() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_wal_statistics',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            wal_records,
            wal_fpi,
            wal_bytes,
            pg_size_pretty(wal_bytes) AS wal_bytes_pretty,
            wal_buffers_full
        FROM pg_stat_wal
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_checkpointer_stats() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_checkpointer_stats',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            num_timed,
            num_requested,
            num_done,
            write_time,
            sync_time,
            buffers_written,
            slru_written
        FROM pg_stat_checkpointer
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_slru_stats() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_slru_stats',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            name,
            blks_zeroed,
            blks_hit,
            blks_read,
            blks_written,
            blks_exists,
            flushes,
            truncates
        FROM pg_stat_slru
        WHERE blks_read > 0 OR blks_written > 0 OR flushes > 0
        ORDER BY blks_read DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_database_conflict_stats() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_database_conflict_stats',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            d.datname,
            c.confl_tablespace,
            c.confl_lock,
            c.confl_snapshot,
            c.confl_bufferpin,
            c.confl_deadlock,
            c.confl_active_logicalslot
        FROM pg_stat_database_conflicts c
        JOIN pg_database d ON c.datid = d.oid
        WHERE c.confl_tablespace + c.confl_lock + c.confl_snapshot + c.confl_bufferpin + c.confl_deadlock > 0
        ORDER BY (c.confl_tablespace + c.confl_lock + c.confl_snapshot + c.confl_bufferpin + c.confl_deadlock) DESC
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_user_function_stats() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_user_function_stats',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            funcid,
            schemaname,
            funcname,
            calls,
            total_time,
            self_time,
            ROUND((total_time / NULLIF(calls, 0))::numeric, 2) AS avg_time_ms
        FROM pg_stat_user_functions
        WHERE calls > 0
        ORDER BY total_time DESC
        LIMIT 10
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_io_statistics_v2() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_io_statistics_v2',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            backend_type,
            object,
            context,
            reads,
            read_bytes,
            pg_size_pretty(read_bytes) AS read_bytes_pretty,
            writes,
            write_bytes,
            pg_size_pretty(write_bytes) AS write_bytes_pretty,
            extends,
            extend_bytes,
            pg_size_pretty(extend_bytes) AS extend_bytes_pretty,
            hits,
            evictions,
            reuses,
            fsyncs,
            fsync_time
        FROM pg_stat_io
        WHERE backend_type IS NOT NULL
        ORDER BY (reads + writes) DESC
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_long_running_transactions() {
    local threshold_hours=${1:-1}
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_long_running_transactions',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            pid,
            usename,
            datname,
            state,
            age(clock_timestamp(), xact_start) AS transaction_duration,
            query
        FROM pg_stat_activity
        WHERE state != 'idle'
          AND xact_start IS NOT NULL
          AND backend_type = 'client backend'
          AND age(clock_timestamp(), xact_start) > ('${threshold_hours} hour')::interval
        ORDER BY xact_start ASC
    ) t;
EOF
)
    execute_sql_as_json "$query" ""
}

function get_bgwriter_stats() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_bgwriter_stats',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            buffers_clean,
            maxwritten_clean,
            buffers_alloc
        FROM pg_stat_bgwriter
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_deadlock_detection() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_deadlock_detection',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            datname,
            deadlocks
        FROM pg_stat_database
        WHERE datname = current_database()
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_multixid_wraparound_risk() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_multixid_wraparound_risk',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            d.datname,
            age(d.datminmxid) AS mxid_age,
            d.datminmxid,
            current_setting('autovacuum_multixact_freeze_max_age')::bigint AS freeze_max_age,
            CASE
                WHEN d.datminmxid = 0 THEN 'FROZEN'
                WHEN age(d.datminmxid) >= 2147483647 THEN 'INVALID_OR_FROZEN'
                WHEN age(d.datminmxid) >= current_setting('autovacuum_multixact_freeze_max_age')::bigint THEN 'FORCE_AUTOVACUUM'
                WHEN age(d.datminmxid) >= 2107483647 THEN 'CRITICAL'
                WHEN age(d.datminmxid) >= 2117483647 THEN 'WARNING'
                ELSE 'OK'
            END AS status,
            CASE
                WHEN d.datminmxid = 0 THEN NULL
                WHEN age(d.datminmxid) >= 2147483647 THEN NULL
                ELSE (current_setting('autovacuum_multixact_freeze_max_age')::bigint - age(d.datminmxid))
            END AS remaining_to_autovacuum
        FROM
            pg_database d
        WHERE
            d.datallowconn = true
        ORDER BY
            mxid_age DESC
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_connection_security_status() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_connection_security_status',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            a.datname,
            a.usename,
            a.client_addr,
            COALESCE(s.ssl, false) AS ssl_enabled,
            COALESCE(s.version, 'N/A') AS ssl_version,
            COALESCE(s.cipher, 'N/A') AS ssl_cipher,
            COALESCE(g.gss_authenticated, false) AS gssapi_auth,
            COALESCE(g.encrypted, false) AS gssapi_encryption,
            CASE
                WHEN s.ssl = true THEN 'SSL'
                WHEN g.encrypted = true THEN 'GSSAPI'
                WHEN a.client_addr IS NULL THEN 'local'
                ELSE 'unencrypted'
            END AS connection_type
        FROM pg_stat_activity a
        LEFT JOIN pg_stat_ssl s ON a.pid = s.pid
        LEFT JOIN pg_stat_gssapi g ON a.pid = g.pid
        WHERE a.backend_type = 'client backend'
        ORDER BY connection_type, a.datname, a.usename
        LIMIT 50
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_total_temp_bytes() {
    local threshold_gb=${1:-1}  # Default threshold 1GB
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_total_temp_bytes',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            datname,
            temp_files,
            temp_bytes,
            pg_size_pretty(temp_bytes) AS temp_bytes_pretty,
            (temp_bytes / (1024.0 * 1024 * 1024))::numeric(10,2) AS temp_bytes_gb
        FROM pg_stat_database
        WHERE temp_bytes > (${threshold_gb} * 1024 * 1024 * 1024)
        ORDER BY temp_bytes DESC
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_checkpointer_write_sync_time() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_checkpointer_write_sync_time',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            num_timed,
            num_requested,
            num_done,
            ROUND(write_time::numeric, 2) AS write_time_ms,
            ROUND(sync_time::numeric, 2) AS sync_time_ms,
            buffers_written,
            CASE
                WHEN num_done > 0 THEN ROUND((write_time / num_done)::numeric, 2)
                ELSE 0
            END AS avg_write_time_per_checkpoint_ms,
            CASE
                WHEN num_done > 0 THEN ROUND((sync_time / num_done)::numeric, 2)
                ELSE 0
            END AS avg_sync_time_per_checkpoint_ms,
            CASE
                WHEN num_timed > 0 AND num_requested > num_timed * 2 THEN 'WARNING'
                WHEN write_time > 10000 OR sync_time > 10000 THEN 'WARNING'
                ELSE 'OK'
            END AS checkpointer_status
        FROM pg_stat_checkpointer
    ) t;
EOF
)
    execute_sql_as_json "$query"
}

function get_lock_waiters() {
    local query=$(cat <<EOF
    SELECT json_build_object(
        'skill', 'get_lock_waiters',
        'status', 'success',
        'data', COALESCE(json_agg(t), '[]'::json)
    )
    FROM (
        SELECT
            blocked_locks.pid AS blocked_pid,
            blocked_activity.usename AS blocked_user,
            blocked_activity.query AS blocked_query,
            blocking_locks.pid AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocking_activity.query AS blocking_query,
            blocked_locks.mode AS blocked_mode,
            blocked_locks.relation::regclass AS blocked_relation
        FROM pg_locks blocked_locks
        JOIN pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
        JOIN pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
            AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
            AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
            AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
            AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
            AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
            AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
            AND blocking_locks.pid != blocked_locks.pid
        JOIN pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
        WHERE NOT blocked_locks.granted
        ORDER BY blocked_locks.pid
        LIMIT 20
    ) t;
EOF
)
    execute_sql_as_json "$query"
}


# --- Main Execution Logic ---
SKILL_NAME=$1

if [ -z "$SKILL_NAME" ]; then
    echo "{\"skill\": \"error\", \"status\": \"fail\", \"data\": \"Skill name not provided.\"}"
    exit 1
fi

# Case statement to call the appropriate function based on the skill name.
case "$SKILL_NAME" in
    get_long_running_queries)
        get_long_running_queries "$2"
        ;; 
    get_idle_in_transaction_sessions)
        get_idle_in_transaction_sessions "$2"
        ;; 
    get_blocking_locks)
        get_blocking_locks
        ;; 
    get_cache_hit_rate)
        get_cache_hit_rate
        ;; 
    get_replication_status)
        get_replication_status
        ;; 
    get_database_sizes)
        get_database_sizes
        ;; 
    get_connection_usage)
        get_connection_usage
        ;; 
    get_xid_wraparound_risk)
        get_xid_wraparound_risk
        ;; 
    get_invalid_indexes)
        get_invalid_indexes
        ;; 
    get_rollback_rate)
        get_rollback_rate
        ;; 
    get_replication_slots)
        get_replication_slots
        ;; 
    get_autovacuum_status)
        get_autovacuum_status
        ;; 
    get_top_sql_by_time)
        get_top_sql_by_time
        ;; 
    get_top_objects_by_size)
        get_top_objects_by_size
        ;; 
    get_table_hotspots)
        get_table_hotspots
        ;; 
    get_wal_archiver_status)
        get_wal_archiver_status
        ;; 
    get_large_unused_indexes)
        get_large_unused_indexes
        ;; 
    get_table_bloat)
        get_table_bloat
        ;; 
    get_index_bloat)
        get_index_bloat
        ;; 
    get_long_running_prepared_transactions)
        get_long_running_prepared_transactions "$2"
        ;; 
    get_logical_replication_status)
        get_logical_replication_status
        ;; 
    get_freeze_prediction)
        get_freeze_prediction
        ;; 
    get_wait_events)
        get_wait_events
        ;;
    get_critical_settings)
        get_critical_settings
        ;;
    get_stale_statistics)
        get_stale_statistics
        ;;
    get_sequence_exhaustion)
        get_sequence_exhaustion
        ;;
    get_temp_file_usage)
        get_temp_file_usage
        ;;
    get_io_statistics)
        get_io_statistics
        ;;
    get_analyze_progress)
        get_analyze_progress
        ;;
    get_create_index_progress)
        get_create_index_progress
        ;;
    get_cluster_progress)
        get_cluster_progress
        ;;
    get_wal_statistics)
        get_wal_statistics
        ;;
    get_checkpointer_stats)
        get_checkpointer_stats
        ;;
    get_slru_stats)
        get_slru_stats
        ;;
    get_database_conflict_stats)
        get_database_conflict_stats
        ;;
    get_user_function_stats)
        get_user_function_stats
        ;;
    get_io_statistics_v2)
        get_io_statistics_v2
        ;;
    get_long_running_transactions)
        get_long_running_transactions "$2"
        ;;
    get_bgwriter_stats)
        get_bgwriter_stats
        ;;
    get_deadlock_detection)
        get_deadlock_detection
        ;;
    get_lock_waiters)
        get_lock_waiters
        ;;
    get_multixid_wraparound_risk)
        get_multixid_wraparound_risk
        ;;
    get_connection_security_status)
        get_connection_security_status
        ;;
    get_total_temp_bytes)
        get_total_temp_bytes "$2"
        ;;
    get_checkpointer_write_sync_time)
        get_checkpointer_write_sync_time
        ;;
    *)
        echo "{\"skill\": \"$SKILL_NAME\", \"status\": \"fail\", \"data\": \"Unknown skill.\"}"
        exit 1
        ;;
esac

exit 0
