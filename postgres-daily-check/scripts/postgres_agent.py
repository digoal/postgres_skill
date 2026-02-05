# -*- coding: utf-8 -*-
import json
import subprocess
import datetime
import os

# =================================================================
# PostgreSQL Daily Check AI Agent
# =================================================================
# This agent orchestrates the daily health check of a PostgreSQL database.
# 1. It defines a list of checks (skills) to be performed.
# 2. It executes each check by calling the `run_postgres_check.sh` script.
# 3. It analyzes the JSON output from the script.
# 4. It generates a human-readable summary report in Markdown format.
# =================================================================


class PostgresAgent:
    """
    The core logic for the PostgreSQL monitoring agent.
    """

    def __init__(self, executor_script="run_postgres_check.sh"):
        self.executor_script = os.path.join(os.path.dirname(__file__), executor_script)
        self.report = []
        self.report_status = "✅ OK"
        self.raw_results = {}  # Store raw SQL results

    def _bytes_to_human_readable(self, num_bytes):
        """Converts bytes to human-readable format (e.g., KB, MB, GB)."""
        if num_bytes is None:
            return "N/A"
        num_bytes = float(num_bytes)
        for unit in ["bytes", "KB", "MB", "GB", "TB"]:
            if abs(num_bytes) < 1024.0:
                return f"{num_bytes:.2f} {unit}"
            num_bytes /= 1024.0
        return f"{num_bytes:.2f} PB"

    def _run_skill(self, skill_name, params=None):
        """
        Executes a skill using the shell script and returns the parsed JSON output.
        """
        command = [self.executor_script, skill_name]
        if params:
            command.extend(params)

        try:
            if not os.access(self.executor_script, os.X_OK):
                os.chmod(self.executor_script, 0o755)

            process = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=900,
                env=os.environ,
            )

            if process.returncode != 0:
                if "does not exist" in process.stderr:
                    return {
                        "skill": skill_name,
                        "status": "success",
                        "data": [],
                        "notes": f"view or table does not exist: {process.stderr}",
                    }
                return {
                    "skill": skill_name,
                    "status": "fail",
                    "data": f"Script execution failed with code {process.returncode}: {process.stderr}",
                }

            if not process.stdout.strip():
                return {"skill": skill_name, "status": "success", "data": []}

            return json.loads(process.stdout)
        except json.JSONDecodeError as e:
            return {
                "skill": skill_name,
                "status": "fail",
                "data": f"Failed to parse JSON output: {e}. Raw output: {process.stdout}",
            }
        except Exception as e:
            return {
                "skill": skill_name,
                "status": "fail",
                "data": f"An unexpected error occurred: {e}",
            }

    def _update_status(self, new_status):
        """Helper to safely elevate the report status."""
        if new_status == "❌ ERROR":
            self.report_status = "❌ ERROR"
        elif new_status == "🟠 WARNING" and self.report_status != "❌ ERROR":
            self.report_status = "🟠 WARNING"

    def _analyze_and_report(self, result):
        """
        Analyzes the result of a skill and adds findings to the report.
        """
        skill = result.get("skill", "unknown_skill")
        status = result.get("status", "fail")
        data = result.get("data", [])
        notes = result.get("notes", "")

        if status != "success":
            self.report.append(f"### ❌ Skill Failed: `{skill}`")
            self.report.append(f"Error details: `{data}`")
            self._update_status("❌ ERROR")
            return

        # --- Analysis Logic ---
        if skill == "get_blocking_locks":
            if data:
                self.report.append("### ❌ ERROR: Blocking Locks Detected")
                self.report.append(f"Found {len(data)} blocking lock situations.")
                for lock in data:
                    self.report.append(
                        f"- **Waiting PID:** {lock['waiting_pid']} is blocked by **Blocking PID:** {lock['blocking_pid']}."
                    )
                self._update_status("❌ ERROR")
            else:
                self.report.append("### ✅ OK: No Blocking Locks")

        elif skill == "get_top_sql_by_time":
            self.report.append("### 🟡 INFO: Top 5 Queries by Total Execution Time")
            if data:
                self.report.append("| Total Mins | Avg ms | Calls | Query |")
                self.report.append("|---|---|---|---|")
                for item in data:
                    query_text = (
                        item["query"].replace("\n", " ").replace("\r", "")[:80] + "..."
                    )
                    self.report.append(
                        f"| {item['total_minutes']} | {item['avg_ms']} | {item['calls']} | `{query_text}` |"
                    )
            elif "does not exist" in notes:
                self.report.append(
                    "`pg_stat_statements` extension is not installed or available."
                )
            else:
                self.report.append("Could not retrieve Top SQL data.")

        elif skill == "get_top_objects_by_size":
            self.report.append("### 🟡 INFO: Top 5 Largest Objects")
            if data:
                self.report.append("| Type | Schema | Name | Size |")
                self.report.append("|---|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item['type']} | {item['schemaname']} | {item['object_name']} | {item['size']} |"
                    )
            else:
                self.report.append("Could not retrieve object size data.")

        elif skill == "get_table_hotspots":
            self.report.append("### 🟡 INFO: Top 5 Table Hotspots (by DMLs & Scans)")
            if data:
                self.report.append(
                    "| Schema | Table | Total DMLs | Total Scans | Dead Tuples |"
                )
                self.report.append("|---|---|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item['schemaname']} | {item['relname']} | {item['total_dml']} | {item['total_scans']} | {item['n_dead_tup']} |"
                    )
            else:
                self.report.append("Could not retrieve table hotspot data.")

        elif skill == "get_wal_archiver_status":
            self.report.append("### 🟡 INFO: WAL & Archiver Status")
            if data:
                status = data[0]
                if status["failed_count"] > 0:
                    self.report.append(f"### ❌ ERROR: Archiver has Failed")
                    self.report.append(f"- **Failed Count:** {status['failed_count']}")
                    self.report.append(
                        f"- **Last Failed WAL:** `{status['last_failed_wal']}` at `{status['last_failed_time']}`"
                    )
                    self._update_status("❌ ERROR")
                else:
                    self.report.append("### ✅ OK: Archiver Status")

                self.report.append(
                    f"- **WAL Directory Size:** {status['wal_directory_size']}"
                )
                self.report.append(
                    f"- **Last Archived WAL:** `{status['last_archived_wal']}` at `{status['last_archived_time']}`"
                )
            elif "does not exist" in notes:
                self.report.append(
                    "Archiving may be disabled (`archive_mode` is likely off)."
                )
            else:
                self.report.append("Could not retrieve archiver status.")

        elif skill == "get_large_unused_indexes":
            self.report.append("### 🟡 INFO: Large Unused Indexes (>10MB)")
            if data:
                self.report.append(
                    f"Found {len(data)} large indexes that have not been scanned. These are candidates for removal, but require careful analysis."
                )
                self.report.append("| Table | Index | Size |")
                self.report.append("|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| `{item['schemaname']}.{item['table_name']}` | `{item['index_name']}` | {item['index_size']} |"
                    )
                self._update_status("🟠 WARNING")
            else:
                self.report.append("No large, unused indexes were found.")

        # --- Bloat Reports ---
        elif skill == "get_table_bloat" or skill == "get_index_bloat":
            obj_type = "Table" if skill == "get_table_bloat" else "Index"
            self.report.append(f"### 🟡 INFO: Top 10 Bloated {obj_type}s")
            if data:
                has_bloat_warning = False
                self.report.append(
                    f"| Schema | {obj_type} Name | Total Size | Bloat % | Wasted Space |"
                )
                self.report.append("|---|---|---|---|---|")
                for item in data:
                    bloat_pct = float(item.get("bloat_percentage", 0))
                    wasted_bytes = float(item.get("wasted_bytes", 0))
                    total_bytes = float(item.get("total_bytes", 0))

                    if bloat_pct > 20 and wasted_bytes > 100 * (
                        1024**2
                    ):  # 20% bloat and > 100MB wasted
                        has_bloat_warning = True
                    self.report.append(
                        f"| {item.get('schemaname', 'N/A')} | `{item.get('tablename', item.get('index_name', 'N/A'))}` | {self._bytes_to_human_readable(total_bytes)} | {bloat_pct:.2f}% | {self._bytes_to_human_readable(wasted_bytes)} |"
                    )
                if has_bloat_warning:
                    # Adjust the previous INFO header to WARNING
                    self.report[len(self.report) - len(data) - 2] = (
                        f"### 🟠 WARNING: Significant {obj_type} Bloat Detected"
                    )
                    self._update_status("🟠 WARNING")
            else:
                self.report.append(
                    f"No significant {obj_type.lower()} bloat detected (or objects are too small/new to check)."
                )

        # --- Other existing skills ---
        elif skill == "get_long_running_queries":
            if data:
                self.report.append("### 🟠 WARNING: Long-Running Queries Detected")
                self.report.append(
                    f"Found {len(data)} queries running longer than the threshold."
                )
                for q in data:
                    self.report.append(
                        f"- **PID:** {q['pid']}, **User:** {q['usename']}, **Duration:** {q['duration']}"
                    )
                self._update_status("🟠 WARNING")
            else:
                self.report.append("### ✅ OK: No Long-Running Queries")

        elif skill == "get_idle_in_transaction_sessions":
            if data:
                self.report.append(
                    "### 🟠 WARNING: Idle-in-Transaction Sessions Detected"
                )
                self.report.append(
                    f"Found {len(data)} sessions idle in transaction longer than the threshold."
                )
                for s in data:
                    self.report.append(
                        f"- **PID:** {s['pid']}, **User:** {s['usename']}, **Duration:** {s['transaction_duration']}"
                    )
                self._update_status("🟠 WARNING")
            else:
                self.report.append("### ✅ OK: No Idle-in-Transaction Sessions")

        elif skill == "get_connection_usage":
            if data:
                used = data[0]["used_connections"]
                max_conn = data[0]["max_connections"]
                usage_percent = (used / max_conn) * 100

                if usage_percent > 95:
                    self.report.append(
                        f"### ❌ ERROR: High Connection Usage ({usage_percent:.1f}%)"
                    )
                    self._update_status("❌ ERROR")
                elif usage_percent > 80:
                    self.report.append(
                        f"### 🟠 WARNING: High Connection Usage ({usage_percent:.1f}%)"
                    )
                    self._update_status("🟠 WARNING")
                else:
                    self.report.append(
                        f"### ✅ OK: Connection Usage ({usage_percent:.1f}%)"
                    )
                self.report.append(f"Current active connections: {used} / {max_conn}")
            else:
                self.report.append("### 🟡 INFO: Connection Usage")

        elif skill == "get_cache_hit_rate":
            if data:
                hit_rate = float(data[0].get("hit_rate_percentage", 0))
                db_name = data[0].get("datname", "N/A")
                if hit_rate < 99.0:
                    self.report.append(
                        f"### 🟠 WARNING: Low Cache Hit Rate for '{db_name}' ({hit_rate}%)"
                    )
                    self._update_status("🟠 WARNING")
                else:
                    self.report.append(
                        f"### ✅ OK: Cache Hit Rate for '{db_name}' ({hit_rate}%)"
                    )
            else:
                self.report.append("### 🟡 INFO: Cache Hit Rate")

        elif skill == "get_xid_wraparound_risk":
            self.report.append("### 🟡 INFO: Transaction ID Wraparound Risk")
            has_risk = False
            for db in data:
                age = db.get("xid_age", 0)
                percent = db.get("percentage_used", 0)
                if age > 1_800_000_000:  # ~85%
                    self.report.append(
                        f"- **{db['datname']}**: ❌ **CRITICAL** - {percent}% used ({age:,} transactions old)"
                    )
                    has_risk = True
                    self._update_status("❌ ERROR")
                elif age > 1_500_000_000:  # ~70%
                    self.report.append(
                        f"- **{db['datname']}:** 🟠 **WARNING** - {percent}% used ({age:,} transactions old)"
                    )
                    has_risk = True
                    self._update_status("🟠 WARNING")
            if not has_risk:
                self.report.append(
                    "All databases are well below the wraparound threshold."
                )

        elif skill == "get_invalid_indexes":
            if data:
                self.report.append("### ❌ ERROR: Invalid Indexes Found")
                self.report.append(
                    "These indexes are unusable and may block DML. Recreate them with `REINDEX` or drop and create them again."
                )
                for idx in data:
                    self.report.append(f"- `{idx['schema_name']}.{idx['index_name']}`")
                self._update_status("❌ ERROR")
            else:
                self.report.append("### ✅ OK: No Invalid Indexes")

        elif skill == "get_rollback_rate":
            self.report.append("### 🟡 INFO: Transaction Rollback Rate")
            has_high_rate = False
            if data:
                for db in data:
                    rate = float(db.get("rollback_percentage", 0))
                    if rate > 5:
                        self.report.append(
                            f"- **{db['datname']}**: 🟠 **WARNING** - Rollback rate is {rate}%. High rollbacks can indicate application logic issues."
                        )
                        has_high_rate = True
                        self._update_status("🟠 WARNING")
            if not has_high_rate:
                self.report.append(
                    "Transaction rollback rates are within normal limits."
                )

        elif skill == "get_replication_slots":
            self.report.append("### 🟡 INFO: Replication Slots Status")
            if not data and result.get("notes") != "view or table does not exist":
                self.report.append("No replication slots found.")
            elif data:
                has_issue = False
                for slot in data:
                    if not slot["active"]:
                        lag_gb = slot["restart_lsn_lag_bytes"] / (1024**3)
                        self.report.append(
                            f"- **{slot['slot_name']}**: ❌ **ERROR** - Slot is INACTIVE, holding back WAL logs by {lag_gb:.2f} GB."
                        )
                        has_issue = True
                        self._update_status("❌ ERROR")
                if not has_issue:
                    self.report.append("All replication slots are active.")
            elif "does not exist" in notes:
                self.report.append(
                    "Replication slots are not applicable or view does not exist."
                )

        elif skill == "get_autovacuum_status":
            self.report.append("### 🟡 INFO: Autovacuum Worker Status")
            if data:
                self.report.append("Found running autovacuum processes:")
                for av in data:
                    duration_str = av.get("duration", "N/A")
                    self.report.append(
                        f"- **PID {av['pid']}**: Running on db `{av['datname']}` for {duration_str}."
                    )
            else:
                self.report.append("No autovacuum workers are currently active.")

        elif skill == "get_replication_status":
            self.report.append("### 🟡 INFO: Replication Status")
            if not data:
                self.report.append(
                    "No active replicas found (normal for a standalone instance)."
                )
            else:
                has_lag = False
                for replica in data:
                    lag_mb = replica.get("replay_lag_bytes", 0) / (1024**2)
                    self.report.append(
                        f"- **Replica:** `{replica.get('client_addr', 'N/A')}`, **State:** `{replica.get('state')}`, **Replay Lag:** `{lag_mb:.2f} MB`"
                    )
                    if lag_mb > 1024:  # Threshold: 1 GB
                        has_lag = True
                        self._update_status("❌ ERROR")
                    elif lag_mb > 100:  # Threshold: 100 MB
                        has_lag = True
                        self._update_status("🟠 WARNING")
                if has_lag:
                    self.report[-len(data) - 1] = (
                        "### 🟠 WARNING: Replication Lag Detected"
                    )

        elif skill == "get_database_sizes":
            self.report.append("### 🟡 INFO: Top 10 Database Sizes")
            if data:
                self.report.append("| Database Name | Size |")
                self.report.append("|---|---|")
                for db in data:
                    self.report.append(f"| {db['datname']} | {db['size']} |")
            else:
                self.report.append("Could not retrieve database sizes.")

        elif skill == "get_freeze_prediction":
            self.report.append(
                "### 🟡 INFO: Freeze Storm Prediction (XID/MXID Wraparound)"
            )
            if data:
                has_critical = False
                has_warning = False
                self.report.append(
                    "| Schema | Table Name | Total Size | XID Remain | MXID Remain | Status |"
                )
                self.report.append("|---|---|---|---|---|---|")
                for item in data:
                    status = item["freeze_status"]
                    if status == "CRITICAL" or status.endswith("_OVERDUE"):
                        has_critical = True
                    elif status == "WARNING":
                        has_warning = True
                    mxid_remain = item.get("mxid_remain_ages")
                    mxid_str = str(mxid_remain) if mxid_remain is not None else "N/A"
                    self.report.append(
                        f"| {item['schemaname']} | `{item['table_name']}` | {item['total_size']} | {item['xid_remain_ages']:,} | {mxid_str} | **{status}** |"
                    )

                if has_critical:
                    self.report[len(self.report) - len(data) - 2] = (
                        "### ❌ ERROR: Critical Freeze Storm Risk Detected!"
                    )
                    self._update_status("❌ ERROR")
                elif has_warning:
                    self.report[len(self.report) - len(data) - 2] = (
                        "### 🟠 WARNING: Freeze Storm Risk Detected"
                    )
                    self._update_status("🟠 WARNING")
            else:
                self.report.append(
                    "No tables are currently approaching XID/MXID freeze limits."
                )

        # --- New Skills Analysis ---
        elif skill == "get_critical_settings":
            self.report.append("### 🟡 INFO: Critical Settings Review")
            if data:
                has_critical = False
                self.report.append("| Setting | Value | Recommendation |")
                self.report.append("|---|---|---|")
                for item in data:
                    setting_name = item["name"]
                    setting_value = item["setting"]
                    recommendation = "OK"
                    if setting_name == "fsync" and setting_value != "on":
                        recommendation = (
                            "❌ **CRITICAL!** Data loss risk. Should be 'on'."
                        )
                        has_critical = True
                        self._update_status("❌ ERROR")
                    elif setting_name == "synchronous_commit" and setting_value not in (
                        "on",
                        "local",
                    ):
                        recommendation = "🟠 **WARNING!** Potential data loss on crash. Default is 'on'."
                        self._update_status("🟠 WARNING")
                    self.report.append(
                        f"| `{setting_name}` | `{setting_value}` | {recommendation} |"
                    )
                if has_critical:
                    self.report[len(self.report) - len(data) - 2] = (
                        "### ❌ ERROR: Critical Settings Misconfiguration"
                    )
            else:
                self.report.append("Could not retrieve critical settings information.")

        elif skill == "get_sequence_exhaustion":
            self.report.append("### 🟡 INFO: Sequence Exhaustion Risk")
            if data:
                self.report.append("### 🟠 WARNING: Sequences Approaching Max Value")
                self.report.append(
                    "The following sequences are over 80% used. Consider changing to a BIGINT or resetting if appropriate."
                )
                self.report.append("| Schema | Sequence Name | Percentage Used |")
                self.report.append("|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item['schemaname']} | `{item['sequence_name']}` | {item['percentage_used']}% |"
                    )
                self._update_status("🟠 WARNING")
            else:
                self.report.append(
                    "No sequences are nearing their exhaustion threshold."
                )

        elif skill == "get_wait_events":
            self.report.append("### 🟡 INFO: Top 10 Current Wait Events")
            if data:
                self.report.append(
                    "Shows what active sessions are waiting for right now. Useful for diagnosing bottlenecks."
                )
                self.report.append("| Wait Event Type | Wait Event | Occurrences |")
                self.report.append("|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item['wait_event_type']} | `{item['wait_event']}` | {item['occurrences']} |"
                    )
            else:
                self.report.append(
                    "No significant wait events detected at this moment."
                )

        elif skill == "get_stale_statistics":
            self.report.append("### 🟡 INFO: Stale Table Statistics")
            if data:
                self.report.append("### 🟠 WARNING: Tables with Stale Statistics Found")
                self.report.append(
                    "The following tables have had >10% of their rows modified since the last ANALYZE. Outdated stats can lead to poor query plans."
                )
                self.report.append(
                    "| Schema | Table Name | Live Tuples | Modified % | Last Auto-Analyze |"
                )
                self.report.append("|---|---|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item['schemaname']} | `{item['relname']}` | {item['n_live_tup']:,} | {item['modified_percent']}% | {item['last_autoanalyze']} |"
                    )
                self._update_status("🟠 WARNING")
            else:
                self.report.append("Table statistics appear to be up-to-date.")

        elif skill == "get_io_statistics":
            self.report.append("### 🟡 INFO: I/O Statistics")
            if data:
                io_stats = data[0]
                temp_files = io_stats.get("temp_files", 0)
                temp_bytes = io_stats.get("temp_bytes", 0)
                blks_read = io_stats.get("blks_read", 0)
                blks_hit = io_stats.get("blks_hit", 0)
                total_blks = io_stats.get("total_blks", 0)
                blk_read_time = io_stats.get("blk_read_time", 0)
                blk_write_time = io_stats.get("blk_write_time", 0)

                self.report.append(f"- **Temp Files:** {temp_files}")
                self.report.append(
                    f"- **Temp Bytes:** {io_stats.get('temp_bytes_pretty', 'N/A')}"
                )
                self.report.append(f"- **Blocks Read:** {blks_read:,}")
                self.report.append(f"- **Blocks Hit:** {blks_hit:,}")
                self.report.append(f"- **Total Blocks:** {total_blks:,}")
                self.report.append(f"- **Read Time (ms):** {blk_read_time}")
                self.report.append(f"- **Write Time (ms):** {blk_write_time}")

                if temp_files > 100:
                    self.report.append("### 🟠 WARNING: High Temp File Usage")
                    self.report.append(
                        "Large number of temp files may indicate inefficient queries or insufficient work_mem."
                    )
                    self._update_status("🟠 WARNING")
            else:
                self.report.append("Unable to retrieve I/O statistics.")

        elif skill == "get_io_statistics_v2":
            self.report.append("### 🟡 INFO: Extended I/O Statistics (pg_stat_io)")
            if data:
                self.report.append(
                    "| Backend Type | Object | Context | Reads | Read Bytes | Writes | Write Bytes |"
                )
                self.report.append("|---|---|---|---|---|---|---|")
                for item in data[:15]:
                    backend = item.get("backend_type", "N/A")
                    obj = item.get("object", "N/A")
                    ctx = item.get("context", "N/A")
                    reads = item.get("reads", 0) or 0
                    writes = item.get("writes", 0) or 0
                    read_bytes = item.get("read_bytes_pretty", "0 bytes")
                    write_bytes = item.get("write_bytes_pretty", "0 bytes")
                    self.report.append(
                        f"| {backend} | {obj} | {ctx} | {reads:,} | {read_bytes} | {writes:,} | {write_bytes} |"
                    )

                client_backend = next(
                    (
                        x
                        for x in data
                        if x.get("backend_type") == "client backend"
                        and x.get("object") == "relation"
                    ),
                    None,
                )
                if client_backend:
                    hit_ratio = 0
                    reads = client_backend.get("reads", 0) or 0
                    hits = client_backend.get("hits", 0) or 0
                    if reads > 0:
                        hit_ratio = (hits / (reads + hits)) * 100
                    self.report.append(f"\n**Client Backend Relation I/O:**")
                    self.report.append(f"- Reads: {reads:,}, Hits: {hits:,}")
                    self.report.append(f"- Hit Ratio: {hit_ratio:.2f}%")
            else:
                self.report.append(
                    "No I/O statistics available (pg_stat_io may not be available in this PostgreSQL version)."
                )

        elif skill == "get_analyze_progress":
            self.report.append("### 🟡 INFO: ANALYZE Progress")
            if data:
                for item in data:
                    phase = item.get("phase", "unknown")
                    progress_pct = item.get("scan_progress_pct", 0)
                    self.report.append(
                        f"- **PID {item.get('pid', 'N/A')}**: Analyzing `{item.get('relname', 'N/A')}` in `{item.get('datname', 'N/A')}`"
                    )
                    self.report.append(f"  - Phase: {phase}")
                    self.report.append(
                        f"  - Progress: {progress_pct}% ({item.get('sample_blks_scanned', 0)}/{item.get('sample_blks_total', 0)} blocks)"
                    )
                    if phase in [
                        "acquiring sample rows",
                        "acquiring inherited sample rows",
                    ]:
                        if (
                            float(progress_pct or 0) < 5.0
                            and item.get("delay_time", 0) > 60000
                        ):
                            self.report.append(
                                "  - ⚠️ WARNING: ANALYZE may be throttled by vacuum_cost_delay"
                            )
                            self._update_status("🟠 WARNING")
            else:
                self.report.append("No ANALYZE operations currently running.")

        elif skill == "get_create_index_progress":
            self.report.append("### 🟡 INFO: CREATE INDEX / REINDEX Progress")
            if data:
                for item in data:
                    phase = item.get("phase", "unknown")
                    self.report.append(
                        f"- **PID {item.get('pid', 'N/A')}**: Creating index `{item.get('index_name', 'N/A')}` on `{item.get('table_name', 'N/A')}`"
                    )
                    self.report.append(f"  - Command: {item.get('command', 'N/A')}")
                    self.report.append(f"  - Phase: {phase}")
                    self.report.append(
                        f"  - Progress: {item.get('blks_done', 0)}/{item.get('blks_total', 0)} blocks, {item.get('tuples_done', 0)}/{item.get('tuples_total', 0)} tuples"
                    )
                    if "waiting for writers" in phase:
                        self.report.append(
                            "  - ⚠️ Waiting for other transactions to release locks"
                        )
            else:
                self.report.append(
                    "No CREATE INDEX or REINDEX operations currently running."
                )

        elif skill == "get_cluster_progress":
            self.report.append("### 🟡 INFO: CLUSTER / VACUUM FULL Progress")
            if data:
                for item in data:
                    phase = item.get("phase", "unknown")
                    self.report.append(
                        f"- **PID {item.get('pid', 'N/A')}**: Clustering `{item.get('relname', 'N/A')}` in `{item.get('datname', 'N/A')}`"
                    )
                    self.report.append(f"  - Command: {item.get('command', 'N/A')}")
                    self.report.append(f"  - Phase: {phase}")
                    self.report.append(
                        f"  - Progress: {item.get('tuples_done', 0)}/{item.get('tuples_total', 0)} tuples"
                    )
                    if phase == "sorting tuples":
                        if item.get("tuples_done", 0) == 0:
                            self.report.append(
                                "  - ⚠️ May indicate insufficient maintenance_work_mem"
                            )
            else:
                self.report.append(
                    "No CLUSTER or VACUUM FULL operations currently running."
                )

        elif skill == "get_wal_statistics":
            self.report.append("### 🟡 INFO: WAL Statistics")
            if data:
                wal = data[0]
                wal_buffers_full = wal.get("wal_buffers_full", 0)
                self.report.append(f"- **WAL Records:** {wal.get('wal_records', 0):,}")
                self.report.append(f"- **WAL FPI:** {wal.get('wal_fpi', 0):,}")
                self.report.append(
                    f"- **WAL Bytes:** {wal.get('wal_bytes_pretty', 'N/A')}"
                )
                self.report.append(f"- **Buffers Full:** {wal_buffers_full:,}")
                self.report.append(f"- **Write Time:** {wal.get('wal_write', 0)} ms")
                self.report.append(f"- **Sync Time:** {wal.get('wal_sync', 0)} ms")
                if wal_buffers_full > 100:
                    self.report.append("### 🟠 WARNING: High wal_buffers_full count")
                    self.report.append(
                        "Consider increasing wal_buffers or optimizing write workload."
                    )
                    self._update_status("🟠 WARNING")
            else:
                self.report.append("Unable to retrieve WAL statistics.")

        elif skill == "get_checkpointer_stats":
            self.report.append("### 🟡 INFO: Checkpointer Statistics")
            if data:
                cp = data[0]
                timed = cp.get("checkpoints_timed", 0)
                requested = cp.get("checkpoints_req", 0)
                write_time = cp.get("checkpoint_write_time", 0)
                sync_time = cp.get("checkpoint_sync_time", 0)
                buffers_written = cp.get("buffers_written", 0)

                self.report.append(f"- **Timed Checkpoints:** {timed}")
                self.report.append(f"- **Requested Checkpoints:** {requested}")
                self.report.append(f"- **Buffers Written:** {buffers_written:,}")
                self.report.append(f"- **Write Time:** {write_time} ms")
                self.report.append(f"- **Sync Time:** {sync_time} ms")

                if requested > timed * 2:
                    self.report.append(
                        "### 🟠 WARNING: High ratio of requested checkpoints"
                    )
                    self.report.append(
                        "Consider tuning max_wal_size or checkpoint_timeout."
                    )
                    self._update_status("🟠 WARNING")
                if write_time > 10000 or sync_time > 10000:
                    self.report.append("### 🟠 WARNING: High checkpoint I/O time")
                    self.report.append(
                        "Consider faster storage or tuning checkpoint segments."
                    )
                    self._update_status("🟠 WARNING")
            else:
                self.report.append("Unable to retrieve checkpointer statistics.")

        elif skill == "get_slru_stats":
            self.report.append("### 🟡 INFO: SLRU Cache Statistics")
            if data:
                self.report.append("| SLRU Name | Hits | Reads | Hit Ratio |")
                self.report.append("|---|---|---|---|")
                for item in data:
                    hits = item.get("blks_hit", 0)
                    reads = item.get("blks_read", 0)
                    total = hits + reads
                    hit_ratio = round(hits / total * 100, 2) if total > 0 else 0
                    self.report.append(
                        f"| {item.get('name', 'N/A')} | {hits:,} | {reads:,} | {hit_ratio}% |"
                    )
                    if hit_ratio < 90 and reads > 1000:
                        self.report.append(
                            f"  - ⚠️ Low hit ratio for {item.get('name', 'N/A')}"
                        )
            else:
                self.report.append("No significant SLRU activity detected.")

        elif skill == "get_database_conflict_stats":
            self.report.append("### 🟡 INFO: Database Conflict Statistics (Standby)")
            if data:
                has_conflicts = False
                for item in data:
                    conflicts = item.get("conflict_all", 0)
                    if conflicts > 0:
                        has_conflicts = True
                        self.report.append(
                            f"- **{item.get('datname', 'N/A')}**: {conflicts:,} conflicts"
                        )
                        self.report.append(
                            f"  - Tablespace: {item.get('conflict_tablespace', 0)}"
                        )
                        self.report.append(f"  - Lock: {item.get('conflict_lock', 0)}")
                        self.report.append(
                            f"  - Snapshot: {item.get('conflict_snapshot', 0)}"
                        )
                        self.report.append(
                            f"  - Bufferpin: {item.get('conflict_bufferpin', 0)}"
                        )
                        self.report.append(
                            f"  - Deadlock: {item.get('conflict_deadlock', 0)}"
                        )
                        if item.get("conflict_snapshot", 0) > 0:
                            self.report.append(
                                "  - ⚠️ Consider increasing hot_standby_feedback"
                            )
                if has_conflicts:
                    self.report.append("### 🟠 WARNING: Standby conflicts detected")
                    self.report.append(
                        "Conflicts may indicate need to tune max_standby_streaming_delay"
                    )
                    self._update_status("🟠 WARNING")
                else:
                    self.report.append("No recovery conflicts detected.")
            else:
                self.report.append("No standby conflict statistics available.")

        elif skill == "get_user_function_stats":
            self.report.append("### 🟡 INFO: User Function Statistics")
            if data:
                self.report.append(
                    "| Function | Calls | Total Time (ms) | Avg Time (ms) |"
                )
                self.report.append("|---|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item.get('schemaname', 'N/A')}.{item.get('funcname', 'N/A')} | {item.get('calls', 0):,} | {item.get('total_time', 0):.2f} | {item.get('avg_time_ms', 0):.2f} |"
                    )
                self.report.append("")
                self.report.append("Top time-consuming functions:")
                for i, item in enumerate(data[:3]):
                    if float(item.get("total_time", 0)) > 1000:
                        self.report.append(
                            f"  {i + 1}. {item.get('schemaname', 'N/A')}.{item.get('funcname', 'N/A')}: {item.get('total_time', 0):.2f} ms total"
                        )
            else:
                self.report.append(
                    "No user function statistics available (track_functions may be off)."
                )

        elif skill == "get_bgwriter_stats":
            self.report.append("### 🟡 INFO: Background Writer Statistics")
            if data:
                bgwriter = data[0]
                maxwritten = bgwriter.get("maxwritten_clean", 0)
                if maxwritten > 0:
                    self.report.append("### 🟠 WARNING: Background Writer Maxwritten")
                    self.report.append(
                        f"Background writer reached max pages limit {maxwritten} times. Consider tuning bgwriter parameters."
                    )
                    self._update_status("🟠 WARNING")
                else:
                    self.report.append("### ✅ OK: Background Writer Normal")
                self.report.append(
                    f"- **Buffers Clean:** {bgwriter.get('buffers_clean', 0)}"
                )
                self.report.append(
                    f"- **Buffers Allocated:** {bgwriter.get('buffers_alloc', 0)}"
                )
                self.report.append(f"- **Maxwritten Clean:** {maxwritten}")

        elif skill == "get_deadlock_detection":
            self.report.append("### 🟡 INFO: Deadlock Detection")
            if data:
                deadlock_info = data[0]
                deadlock_count = deadlock_info.get("deadlock_count", 0)
                if deadlock_count > 0:
                    self.report.append("### ❌ ERROR: Deadlocks Detected!")
                    self.report.append(f"- **Total Deadlocks:** {deadlock_count}")
                    self.report.append(
                        "Deadlocks have occurred. Check PostgreSQL logs for details."
                    )
                    self._update_status("❌ ERROR")
                else:
                    self.report.append("### ✅ OK: No Deadlocks Detected")
                    self.report.append(f"- **Deadlock Count:** {deadlock_count}")
            else:
                self.report.append("Unable to retrieve deadlock statistics.")

        elif skill == "get_lock_waiters":
            self.report.append("### 🟡 INFO: Lock Waiters (Potential Deadlock Risk)")
            if data:
                if len(data) > 5:
                    self.report.append("### 🟠 WARNING: Multiple Lock Waiters Detected")
                    self.report.append(
                        f"Found {len(data)} sessions waiting for locks. This may indicate potential deadlock risks."
                    )
                    self._update_status("🟠 WARNING")
                else:
                    self.report.append("### ✅ OK: Few Lock Waiters")
                self.report.append(
                    "| Blocked PID | Blocked User | Blocked Query | Blocking PID | Blocking User | Blocked Mode | Relation |"
                )
                self.report.append("|---|---|---|---|---|---|---|")
                for item in data:
                    blocked_query = str(item.get("blocked_query", ""))[:60].replace(
                        "\n", " "
                    )
                    blocking_query = str(item.get("blocking_query", ""))[:60].replace(
                        "\n", " "
                    )
                    self.report.append(
                        f"| {item.get('blocked_pid', 'N/A')} | {item.get('blocked_user', 'N/A')} | `{blocked_query}` | "
                        f"{item.get('blocking_pid', 'N/A')} | {item.get('blocking_user', 'N/A')} | {item.get('blocked_mode', 'N/A')} | {item.get('blocked_relation', 'N/A')} |"
                    )
            else:
                self.report.append("### ✅ OK: No Lock Waiters Detected")

        elif skill == "get_multixid_wraparound_risk":
            self.report.append("### 🟡 INFO: MultiXactId Wraparound Risk")
            has_risk = False
            if data:
                for db in data:
                    status = db.get("status", "OK")
                    datname = db.get("datname", "N/A")
                    mxid_age = db.get("mxid_age", 0)
                    remaining = db.get("remaining_to_autovacuum")
                    
                    if status == "INVALID_OR_FROZEN":
                        self.report.append(
                            f"- **{datname}**: ✅ **FROZEN/INVALID** - datminmxid is frozen or invalid (no risk)"
                        )
                    elif status == "FROZEN":
                        self.report.append(
                            f"- **{datname}**: ✅ **FROZEN** - MultiXactIds are frozen (no risk)"
                        )
                    elif status == "FORCE_AUTOVACUUM":
                        self.report.append(
                            f"- **{datname}**: 🟠 **FORCE AUTOVACUUM** - Autovacuum will be forced ({remaining:,} remaining)"
                        )
                        has_risk = True
                        self._update_status("🟠 WARNING")
                    elif status == "CRITICAL":
                        self.report.append(
                            f"- **{datname}**: ❌ **CRITICAL** - Approaching wraparound ({mxid_age:,} age)"
                        )
                        has_risk = True
                        self._update_status("❌ ERROR")
                    elif status == "WARNING":
                        self.report.append(
                            f"- **{datname}**: 🟠 **WARNING** - Getting close to wraparound ({mxid_age:,} age)"
                        )
                        has_risk = True
                        self._update_status("🟠 WARNING")
                    else:
                        self.report.append(
                            f"- **{datname}**: ✅ **OK** - {remaining:,} MultiXactIds remaining before forced autovacuum"
                        )
            if not has_risk:
                self.report.append("All databases are well below the MultiXactId wraparound threshold.")

        elif skill == "get_connection_security_status":
            self.report.append("### 🟡 INFO: Connection Security Status (SSL/GSSAPI)")
            if data:
                unencrypted = [d for d in data if d.get("connection_type") == "unencrypted"]
                ssl_count = len([d for d in data if d.get("ssl_enabled") == True])
                gssapi_count = len([d for d in data if d.get("gssapi_encryption") == True])
                local_count = len([d for d in data if d.get("connection_type") == "local"])
                
                self.report.append(f"- **SSL Encrypted:** {ssl_count} connections")
                self.report.append(f"- **GSSAPI Encrypted:** {gssapi_count} connections")
                self.report.append(f"- **Local (Unix Socket):** {local_count} connections")
                self.report.append(f"- **Unencrypted (TCP):** {len(unencrypted)} connections")
                
                if unencrypted:
                    self.report.append("### 🟠 WARNING: Unencrypted Remote Connections Detected")
                    self.report.append("The following connections are not encrypted:")
                    self.report.append("| Database | User | Client Address | Connection Type |")
                    self.report.append("|---|---|---|---|")
                    for conn in unencrypted[:10]:  # Show first 10
                        self.report.append(
                            f"| {conn.get('datname', 'N/A')} | {conn.get('usename', 'N/A')} | "
                            f"{conn.get('client_addr', 'N/A')} | {conn.get('connection_type', 'N/A')} |"
                        )
                    self._update_status("🟠 WARNING")
            else:
                self.report.append("No connection security data available.")

        elif skill == "get_total_temp_bytes":
            self.report.append("### 🟡 INFO: Total Temp Bytes Usage")
            if data:
                total_gb = sum(float(item.get("temp_bytes_gb", 0)) for item in data)
                self.report.append(f"**Total Temp Space Used:** {total_gb:.2f} GB")
                self.report.append("")
                self.report.append("| Database | Temp Files | Temp Size |")
                self.report.append("|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item.get('datname', 'N/A')} | {item.get('temp_files', 0):,} | "
                        f"{item.get('temp_bytes_pretty', 'N/A')} |"
                    )
                if total_gb > 10:  # More than 10GB
                    self.report.append("### 🟠 WARNING: High Temporary File Usage")
                    self.report.append("Large temporary file usage may indicate insufficient work_mem or inefficient queries.")
                    self._update_status("🟠 WARNING")
            else:
                self.report.append("No databases exceed the temp bytes threshold.")

        elif skill == "get_checkpointer_write_sync_time":
            self.report.append("### 🟡 INFO: Checkpointer Write/Sync Time Analysis")
            if data:
                cp = data[0]
                write_time = cp.get("write_time_ms", 0)
                sync_time = cp.get("sync_time_ms", 0)
                avg_write = cp.get("avg_write_time_per_checkpoint_ms", 0)
                avg_sync = cp.get("avg_sync_time_per_checkpoint_ms", 0)
                num_timed = cp.get("num_timed", 0)
                num_requested = cp.get("num_requested", 0)
                status = cp.get("checkpointer_status", "OK")
                
                self.report.append(f"- **Total Write Time:** {write_time:,.2f} ms")
                self.report.append(f"- **Total Sync Time:** {sync_time:,.2f} ms")
                self.report.append(f"- **Avg Write per Checkpoint:** {avg_write:,.2f} ms")
                self.report.append(f"- **Avg Sync per Checkpoint:** {avg_sync:,.2f} ms")
                self.report.append(f"- **Timed Checkpoints:** {num_timed}")
                self.report.append(f"- **Requested Checkpoints:** {num_requested}")
                
                if status == "WARNING":
                    if num_requested > num_timed * 2:
                        self.report.append("### 🟠 WARNING: High Requested Checkpoint Ratio")
                        self.report.append("Too many requested checkpoints vs timed checkpoints. Consider increasing max_wal_size.")
                        self._update_status("🟠 WARNING")
                    if avg_write > 5000 or avg_sync > 5000:
                        self.report.append("### 🟠 WARNING: High Checkpoint I/O Time")
                        self.report.append("Average checkpoint write/sync time is high. Consider faster storage or checkpoint tuning.")
                        self._update_status("🟠 WARNING")
            else:
                self.report.append("Unable to retrieve checkpointer statistics.")

        elif skill == "get_logical_replication_status":
            self.report.append("### 🟡 INFO: Logical Replication Status")
            if data:
                has_lag = False
                self.report.append("| Subscription | Send Lag (sec) | Receive Lag (sec) |")
                self.report.append("|---|---|---|")
                for item in data:
                    send_lag = item.get("send_lag_sec", 0)
                    recv_lag = item.get("receive_lag_sec", 0)
                    self.report.append(
                        f"| {item.get('subname', 'N/A')} | {send_lag:.2f} | {recv_lag:.2f} |"
                    )
                    if send_lag > 300 or recv_lag > 300:  # > 5 minutes
                        has_lag = True
                if has_lag:
                    self.report.append("### 🟠 WARNING: Logical Replication Lag Detected")
                    self.report.append("Replication lag exceeds 5 minutes. Check network or subscriber performance.")
                    self._update_status("🟠 WARNING")
            else:
                self.report.append("No logical replication subscriptions found.")

        elif skill == "get_long_running_prepared_transactions":
            self.report.append("### 🟡 INFO: Long-Running Prepared Transactions (2PC)")
            if data:
                self.report.append("### 🟠 WARNING: Long-Running Prepared Transactions Detected")
                self.report.append(
                    f"Found {len(data)} prepared transactions older than threshold. These hold locks and prevent WAL cleanup."
                )
                self.report.append("| GID | Owner | Database | Duration |")
                self.report.append("|---|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item.get('gid', 'N/A')} | {item.get('owner', 'N/A')} | "
                        f"{item.get('database', 'N/A')} | {item.get('duration', 'N/A')} |"
                    )
                self._update_status("🟠 WARNING")
            else:
                self.report.append("### ✅ OK: No Long-Running Prepared Transactions")

        elif skill == "get_long_running_transactions":
            self.report.append("### 🟡 INFO: Long-Running Transactions")
            if data:
                self.report.append("### 🟠 WARNING: Long-Running Transactions Detected")
                self.report.append(
                    f"Found {len(data)} transactions running longer than threshold. These may hold locks and prevent vacuum."
                )
                self.report.append("| PID | User | Database | Duration | State |")
                self.report.append("|---|---|---|---|---|")
                for item in data:
                    query_text = str(item.get("query", ""))[:50].replace("\n", " ")
                    self.report.append(
                        f"| {item.get('pid', 'N/A')} | {item.get('usename', 'N/A')} | "
                        f"{item.get('datname', 'N/A')} | {item.get('transaction_duration', 'N/A')} | "
                        f"{item.get('state', 'N/A')} |"
                    )
                self._update_status("🟠 WARNING")
            else:
                self.report.append("### ✅ OK: No Long-Running Transactions")

        elif skill == "get_temp_file_usage":
            self.report.append("### 🟡 INFO: Temporary File Usage by Database")
            if data:
                self.report.append("| Database | Temp Files | Temp Size | Temp Files Ratio |")
                self.report.append("|---|---|---|---|")
                for item in data:
                    self.report.append(
                        f"| {item.get('datname', 'N/A')} | {item.get('temp_files', 0):,} | "
                        f"{item.get('temp_bytes_pretty', 'N/A')} | {item.get('temp_files_ratio', 0):.2%} |"
                    )
                total_temp_files = sum(item.get("temp_files", 0) for item in data)
                if total_temp_files > 100:
                    self.report.append(f"\n**Total Temp Files:** {total_temp_files:,}")
                    self.report.append("### 🟠 WARNING: High Temporary File Count")
                    self.report.append("High temp file usage may indicate inefficient queries or insufficient work_mem.")
                    self._update_status("🟠 WARNING")
            else:
                self.report.append("No temporary file usage detected.")

    def run_checks(self):
        """
        Run a predefined sequence of checks.
        """
        print("Starting comprehensive PostgreSQL health check...")

        # Define the checklist of skills to run, ordered by importance
        checklist = [
            "get_invalid_indexes",
            "get_xid_wraparound_risk",
            "get_multixid_wraparound_risk",
            "get_freeze_prediction",
            "get_blocking_locks",
            "get_long_running_prepared_transactions",
            "get_long_running_transactions",
            "get_critical_settings",
            "get_sequence_exhaustion",
            "get_replication_slots",
            "get_wal_archiver_status",
            "get_logical_replication_status",
            "get_replication_status",
            "get_wait_events",
            "get_long_running_queries",
            "get_idle_in_transaction_sessions",
            "get_connection_usage",
            "get_cache_hit_rate",
            "get_rollback_rate",
            "get_top_sql_by_time",
            "get_stale_statistics",
            "get_large_unused_indexes",
            "get_autovacuum_status",
            "get_bgwriter_stats",
            "get_temp_file_usage",
            "get_total_temp_bytes",
            "get_io_statistics",
            "get_io_statistics_v2",
            "get_analyze_progress",
            "get_create_index_progress",
            "get_cluster_progress",
            "get_wal_statistics",
            "get_checkpointer_stats",
            "get_checkpointer_write_sync_time",
            "get_slru_stats",
            "get_database_conflict_stats",
            "get_user_function_stats",
            "get_deadlock_detection",
            "get_lock_waiters",
            "get_connection_security_status",
            "get_table_hotspots",
            "get_top_objects_by_size",
            "get_table_bloat",
            "get_index_bloat",
            "get_database_sizes",
        ]

        for skill in checklist:
            print(f"  -> Running skill: {skill}...")
            result = self._run_skill(skill)
            self.raw_results[skill] = result  # Store raw result
            self._analyze_and_report(result)
            self.report.append("\n---\n")  # Separator

        print("Checks complete. Generating report...")
        self.generate_report()

    def generate_report(self):
        """
        Generates the final markdown report and raw JSON data.
        """
        report_title = f"# PostgreSQL Health Report - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        overall_status = f"## Overall Status: {self.report_status}"

        # Clean up extra separators
        while self.report and self.report[-1] == "\n---\n":
            self.report.pop()

        report_content = "\n".join([report_title, overall_status, *self.report])

        # Save markdown report
        report_filename = "daily_health_report.md"
        with open(report_filename, "w", encoding="utf-8") as f:
            f.write(report_content)

        print(f"Report saved to: {report_filename}")

        # Save raw JSON results
        raw_data_filename = "daily_health_raw_data.json"
        raw_data = {
            "generated_at": datetime.datetime.now().isoformat(),
            "overall_status": self.report_status,
            "results": self.raw_results
        }
        with open(raw_data_filename, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, indent=2, ensure_ascii=False, default=str)

        print(f"Raw data saved to: {raw_data_filename}")


if __name__ == "__main__":
    agent = PostgresAgent()
    agent.run_checks()
