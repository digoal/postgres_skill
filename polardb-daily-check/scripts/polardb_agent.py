#!/usr/bin/env python3
"""
PolarDB for PostgreSQL Daily Health Check Agent

This script performs a comprehensive daily health check of a PolarDB instance,
leveraging PolarDB-specific monitoring capabilities and standard PostgreSQL
health checks. It generates a detailed markdown report with actionable insights.

Usage:
    python3 polardb_agent.py [--config CONFIG_FILE] [--output OUTPUT_FILE]
"""

import json
import subprocess
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any


# ANSI colors
class Colors:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    END = "\033[0m"
    BOLD = "\033[1m"


class PolarDBCheckAgent:
    """PolarDB Daily Health Check Agent"""

    def __init__(self, config_file: str = None, output_file: str = None):
        """Initialize the agent with configuration and output settings."""
        self.script_dir = Path(__file__).parent.resolve()
        self.config_file = config_file or str(
            self.script_dir / "../assets/db_config.env"
        )
        self.output_file = output_file or str(
            self.script_dir / "../polar_daily_health_report.md"
        )

        self.check_script = str(self.script_dir / "run_polardb_check.sh")
        self.config = self._load_config()
        self.results: Dict[str, Any] = {}
        self.issues: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []

    def _load_config(self) -> Dict[str, str]:
        """Load database configuration from env file."""
        config = {}
        if os.path.exists(self.config_file):
            with open(self.config_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        config[key.strip()] = value.strip()
        return config

    def run_check(self, check_name: str) -> Dict[str, Any]:
        """Execute a single check using the bash script."""
        try:
            result = subprocess.run(
                [self.check_script, check_name],
                capture_output=True,
                text=True,
                timeout=60,
            )

            output = result.stdout.strip()
            if not output:
                output = result.stderr.strip()

            # Try to parse JSON
            try:
                data = json.loads(output)
            except json.JSONDecodeError:
                data = {"skill": check_name, "status": "unknown", "raw_output": output}

            return data

        except subprocess.TimeoutExpired:
            return {
                "skill": check_name,
                "status": "timeout",
                "message": "Check timed out after 60 seconds",
            }
        except Exception as e:
            return {"skill": check_name, "status": "error", "message": str(e)}

    def run_all_checks(self) -> Dict[str, Any]:
        """Execute all PolarDB and PostgreSQL compatibility checks."""
        print(f"{Colors.CYAN}{'=' * 60}{Colors.END}")
        print(f"{Colors.CYAN}PolarDB Daily Health Check{Colors.END}")
        print(f"{Colors.CYAN}{'=' * 60}{Colors.END}")
        print()

        # PolarDB Core Health Checks
        print(f"{Colors.BLUE}Running PolarDB Core Health Checks...{Colors.END}")
        polardb_core_checks = [
            "get_polar_node_type",
            "get_logindex_status",
            "get_pfs_usage",
            "get_polar_process_status",
            "get_polar_activity",
        ]

        for check in polardb_core_checks:
            print(f"  {Colors.WHITE}→{Colors.END} {check}")
            self.results[check] = self.run_check(check)

        print()

        # HTAP & MPP Checks
        print(f"{Colors.BLUE}Running HTAP & MPP Checks...{Colors.END}")
        htap_checks = [
            "get_px_workers_status",
            "get_px_query_stats",
            "get_px_nodes",
            "get_buffer_pool_affinity",
        ]

        for check in htap_checks:
            print(f"  {Colors.WHITE}→{Colors.END} {check}")
            self.results[check] = self.run_check(check)

        print()

        # Storage & I/O Checks
        print(f"{Colors.BLUE}Running Storage & I/O Checks...{Colors.END}")
        storage_checks = [
            "get_shared_storage_stats",
            "get_polar_io_stats",
            "get_dirty_page_status",
        ]

        for check in storage_checks:
            print(f"  {Colors.WHITE}→{Colors.END} {check}")
            self.results[check] = self.run_check(check)

        print()

        # High Availability Checks
        print(f"{Colors.BLUE}Running High Availability Checks...{Colors.END}")
        ha_checks = [
            "get_primary_readonly_sync",
            "get_online_promote_status",
            "get_recovery_progress",
        ]

        for check in ha_checks:
            print(f"  {Colors.WHITE}→{Colors.END} {check}")
            self.results[check] = self.run_check(check)

        print()

        # PostgreSQL Compatibility Checks
        print(f"{Colors.BLUE}Running PostgreSQL Compatibility Checks...{Colors.END}")
        pg_checks = [
            "get_connection_usage",
            "get_cache_hit_rate",
            "get_long_running_queries",
            "get_idle_in_transaction_sessions",
            "get_replication_status",
            "get_replication_slots",
            "get_autovacuum_status",
            "get_wal_archiver_status",
        ]

        for check in pg_checks:
            print(f"  {Colors.WHITE}→{Colors.END} {check}")
            self.results[check] = self.run_check(check)

        print()
        print(f"{Colors.GREEN}All checks completed.{Colors.END}")
        print()

        return self.results

    def analyze_results(self):
        """Analyze check results and identify issues."""

        # Analyze PolarDB-specific results
        if "get_polar_node_type" in self.results:
            data = self.results["get_polar_node_type"]
            if data.get("status") == "success" and data.get("data"):
                node_info = data["data"][0] if data["data"] else {}
                self.results["node_info"] = node_info

        # Analyze LogIndex status
        if "get_logindex_status" in self.results:
            data = self.results["get_logindex_status"]
            if data.get("status") == "success" and data.get("data"):
                lag_info = data["data"][0] if data["data"] else {}
                lag_bytes = lag_info.get("lag_bytes", 0)
                lag_mb = lag_info.get("lag_mb", 0)

                if lag_bytes > 1073741824:  # > 1GB
                    self.issues.append(
                        {
                            "type": "critical",
                            "check": "get_logindex_status",
                            "message": f"LogIndex replay lag is critical: {lag_mb}MB",
                            "recommendation": "Check storage I/O performance and network bandwidth between compute and storage nodes",
                        }
                    )
                elif lag_bytes > 104857600:  # > 100MB
                    self.warnings.append(
                        {
                            "type": "warning",
                            "check": "get_logindex_status",
                            "message": f"LogIndex replay lag is elevated: {lag_mb}MB",
                            "recommendation": "Monitor storage I/O performance",
                        }
                    )

        # Analyze PFS usage
        if "get_pfs_usage" in self.results:
            data = self.results["get_pfs_usage"]
            if data.get("status") == "success" and data.get("data"):
                storage_info = data["data"][0] if data["data"] else {}
                # Check for storage metrics

        # Analyze connection usage
        if "get_connection_usage" in self.results:
            data = self.results["get_connection_usage"]
            if data.get("status") == "success" and data.get("data"):
                conn_info = data["data"][0] if data["data"] else {}
                current = conn_info.get("current_connections", 0)
                max_conn = conn_info.get("max_connections", 100)
                usage_pct = (current / max_conn) * 100 if max_conn > 0 else 0

                if usage_pct > 95:
                    self.issues.append(
                        {
                            "type": "critical",
                            "check": "get_connection_usage",
                            "message": f"Connection usage is critical: {current}/{max_conn} ({usage_pct:.1f}%)",
                            "recommendation": "Increase max_connections or optimize connection pooling",
                        }
                    )
                elif usage_pct > 80:
                    self.warnings.append(
                        {
                            "type": "warning",
                            "check": "get_connection_usage",
                            "message": f"Connection usage is high: {current}/{max_conn} ({usage_pct:.1f}%)",
                            "recommendation": "Monitor connection growth and consider connection pooling",
                        }
                    )

        # Analyze cache hit rate
        if "get_cache_hit_rate" in self.results:
            data = self.results["get_cache_hit_rate"]
            if data.get("status") == "success" and data.get("data"):
                cache_info = data["data"][0] if data["data"] else {}
                hit_ratio = cache_info.get("hit_ratio", 100)

                if hit_ratio < 99:
                    self.warnings.append(
                        {
                            "type": "warning",
                            "check": "get_cache_hit_rate",
                            "message": f"Cache hit rate is low: {hit_ratio}%",
                            "recommendation": "Increase shared_buffers or optimize queries",
                        }
                    )

        # Analyze long-running queries
        if "get_long_running_queries" in self.results:
            data = self.results["get_long_running_queries"]
            if data.get("status") == "success" and data.get("data"):
                query_info = data["data"][0] if data["data"] else {}
                count = query_info.get("count", 0)

                if count > 0:
                    self.warnings.append(
                        {
                            "type": "warning",
                            "check": "get_long_running_queries",
                            "message": f"Found {count} long-running queries (>5 minutes)",
                            "recommendation": "Review and optimize slow queries",
                        }
                    )

        # Analyze replication status
        if "get_replication_status" in self.results:
            data = self.results["get_replication_status"]
            if data.get("status") == "success" and data.get("data"):
                rep_info = data["data"][0] if data["data"] else {}
                lag_bytes = rep_info.get("replication_lag_bytes", 0)
                lag_mb = lag_bytes / 1024 / 1024

                if lag_bytes > 1073741824:  # > 1GB
                    self.issues.append(
                        {
                            "type": "critical",
                            "check": "get_replication_status",
                            "message": f"Replication lag is critical: {lag_mb:.1f}MB",
                            "recommendation": "Check storage I/O and network performance",
                        }
                    )
                elif lag_bytes > 104857600:  # > 100MB
                    self.warnings.append(
                        {
                            "type": "warning",
                            "check": "get_replication_status",
                            "message": f"Replication lag is elevated: {lag_mb:.1f}MB",
                            "recommendation": "Monitor replication health",
                        }
                    )

        # Analyze WAL archiving
        if "get_wal_archiver_status" in self.results:
            data = self.results["get_wal_archiver_status"]
            if data.get("status") == "success" and data.get("data"):
                arch_info = data["data"][0] if data["data"] else {}
                failed = arch_info.get("failed_archives", 0)

                if failed > 0:
                    self.issues.append(
                        {
                            "type": "critical",
                            "check": "get_wal_archiver_status",
                            "message": f"WAL archiving has {failed} failures",
                            "recommendation": "Check archive_command configuration and storage availability",
                        }
                    )

    def generate_report(self) -> str:
        """Generate markdown health report."""

        report_lines = []

        # Header
        report_lines.append(f"# PolarDB Daily Health Report")
        report_lines.append(f"")
        report_lines.append(
            f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        report_lines.append(f"")

        # Overall Status
        if self.issues:
            overall_status = f"{Colors.RED}CRITICAL{Colors.END}"
        elif self.warnings:
            overall_status = f"{Colors.YELLOW}WARNING{Colors.END}"
        else:
            overall_status = f"{Colors.GREEN}HEALTHY{Colors.END}"

        report_lines.append(f"## Overall Status: {overall_status}")
        report_lines.append(f"")

        # Summary counts
        report_lines.append(f"### Summary")
        report_lines.append(f"- Critical Issues: {len(self.issues)}")
        report_lines.append(f"- Warnings: {len(self.warnings)}")
        report_lines.append(f"- Checks Performed: {len(self.results)}")
        report_lines.append(f"")

        # PolarDB Status Section
        report_lines.append(f"## PolarDB-Specific Status")
        report_lines.append(f"")

        if "get_polar_node_type" in self.results:
            data = self.results["get_polar_node_type"]
            if data.get("status") == "success" and data.get("data"):
                node_info = data["data"][0] if data["data"] else {}
                report_lines.append(f"### Node Information")
                report_lines.append(
                    f"- **Node Type:** {node_info.get('node_type', 'Unknown')}"
                )
                report_lines.append(
                    f"- **PolarDB Version:** {node_info.get('polar_version', 'Unknown')}"
                )
                report_lines.append(f"")

        if "get_logindex_status" in self.results:
            data = self.results["get_logindex_status"]
            report_lines.append(f"### LogIndex Status")
            if data.get("status") == "success" and data.get("data"):
                lag_info = data["data"][0] if data["data"] else {}
                lag_bytes = lag_info.get("lag_bytes", 0)
                lag_mb = lag_bytes / 1024 / 1024 if lag_bytes > 0 else 0
                report_lines.append(f"- **Replay Lag:** {lag_mb:.2f} MB")
                report_lines.append(
                    f"- **Node Role:** {lag_info.get('node_role', 'Unknown')}"
                )
            else:
                report_lines.append(f"- Status: {data.get('message', 'Unknown')}")
            report_lines.append(f"")

        if "get_pfs_usage" in self.results:
            data = self.results["get_pfs_usage"]
            report_lines.append(f"### Storage Usage")
            if data.get("status") == "success" and data.get("data"):
                storage_info = data["data"][0] if data["data"] else {}
                db_size_mb = storage_info.get("database_size_mb", 0)
                report_lines.append(f"- **Database Size:** {db_size_mb} MB")
                note = storage_info.get("note", "")
                if note:
                    report_lines.append(f"- **Note:** {note}")
            report_lines.append(f"")

        if "get_px_workers_status" in self.results:
            data = self.results["get_px_workers_status"]
            report_lines.append(f"### MPP/HTAP Status")
            if data.get("status") == "success" and data.get("data"):
                px_info = data["data"][0] if data["data"] else {}
                report_lines.append(
                    f"- **MPP Enabled:** {px_info.get('polar_enable_px', 'Unknown')}"
                )
                report_lines.append(
                    f"- **Max Workers:** {px_info.get('polar_px_max_workers_number', 'Unknown')}"
                )
                report_lines.append(
                    f"- **DOP per Node:** {px_info.get('polar_px_dop_per_node', 'Unknown')}"
                )
                report_lines.append(
                    f"- **Active Parallel Queries:** {px_info.get('active_parallel_queries', 0)}"
                )
            report_lines.append(f"")

        # Critical Issues
        if self.issues:
            report_lines.append(f"## 🔴 Critical Issues")
            report_lines.append(f"")
            for i, issue in enumerate(self.issues, 1):
                report_lines.append(f"### {i}. {issue['check']}")
                report_lines.append(f"- **Message:** {issue['message']}")
                report_lines.append(f"- **Recommendation:** {issue['recommendation']}")
                report_lines.append(f"")

        # Warnings
        if self.warnings:
            report_lines.append(f"## 🟡 Warnings")
            report_lines.append(f"")
            for i, warning in enumerate(self.warnings, 1):
                report_lines.append(f"### {i}. {warning['check']}")
                report_lines.append(f"- **Message:** {warning['message']}")
                report_lines.append(
                    f"- **Recommendation:** {warning['recommendation']}"
                )
                report_lines.append(f"")

        # Detailed Check Results
        report_lines.append(f"## Detailed Check Results")
        report_lines.append(f"")

        for check_name, result in self.results.items():
            status = result.get("status", "unknown")
            status_icon = (
                "✅"
                if status == "success"
                else (
                    "⚠️" if status == "warning" else ("❌" if status == "error" else "ℹ️")
                )
            )

            report_lines.append(f"### {check_name} {status_icon}")
            report_lines.append(f"- **Status:** {status}")

            message = result.get("message", "")
            if message:
                report_lines.append(f"- **Message:** {message}")

            if result.get("data"):
                data_str = json.dumps(result["data"], indent=2)
                report_lines.append(f"- **Data:**")
                report_lines.append(f"```json")
                report_lines.append(data_str)
                report_lines.append(f"```")

            report_lines.append(f"")

        # Recommendations
        report_lines.append(f"## Recommendations")
        report_lines.append(f"")
        report_lines.append(
            f"Based on the health check results, consider the following actions:"
        )
        report_lines.append(f"")

        if self.issues or self.warnings:
            report_lines.append(f"### Immediate Actions")
            for issue in self.issues:
                report_lines.append(
                    f"- 🔴 **{issue['check']}:** {issue['recommendation']}"
                )

            for warning in self.warnings:
                report_lines.append(
                    f"- 🟡 **{warning['check']}:** {warning['recommendation']}"
                )
        else:
            report_lines.append(
                f"- ✅ System appears healthy. Continue regular monitoring."
            )

        report_lines.append(f"")
        report_lines.append(f"### Preventive Maintenance")
        report_lines.append(f"- Review slow queries using pg_stat_statements")
        report_lines.append(f"- Monitor LogIndex replay lag trends")
        report_lines.append(f"- Plan for storage capacity as usage grows")
        report_lines.append(f"- Test online promotion periodically")
        report_lines.append(f"- Review MPP query performance regularly")

        # Footer
        report_lines.append(f"")
        report_lines.append(f"---")
        report_lines.append(f"*Report generated by PolarDB Daily Check Agent*")
        report_lines.append(f"*Next check scheduled for tomorrow*")

        return "\n".join(report_lines)

    def save_report(self, report: str):
        """Save report to file."""
        with open(self.output_file, "w") as f:
            f.write(report)
        print(f"{Colors.GREEN}Report saved to: {self.output_file}{Colors.END}")

    def run(self):
        """Execute the full health check workflow."""
        print()

        # Run all checks
        self.run_all_checks()

        # Analyze results
        print(f"{Colors.CYAN}Analyzing results...{Colors.END}")
        self.analyze_results()

        # Generate report
        print(f"{Colors.CYAN}Generating report...{Colors.END}")
        report = self.generate_report()

        # Save report
        self.save_report(report)

        # Print summary
        print()
        print(f"{Colors.CYAN}{'=' * 60}{Colors.END}")
        print(f"{Colors.CYAN}Health Check Summary{Colors.END}")
        print(f"{Colors.CYAN}{'=' * 60}{Colors.END}")
        print()

        if self.issues:
            print(f"{Colors.RED}❌ Critical Issues: {len(self.issues)}{Colors.END}")
            for issue in self.issues:
                print(f"   - {issue['check']}: {issue['message']}")
            print()

        if self.warnings:
            print(f"{Colors.YELLOW}⚠️  Warnings: {len(self.warnings)}{Colors.END}")
            for warning in self.warnings:
                print(f"   - {warning['check']}: {warning['message']}")
            print()

        if not self.issues and not self.warnings:
            print(f"{Colors.GREEN}✅ System is healthy!{Colors.END}")
            print()

        # Print report to console (optional, can be disabled)
        print()
        print(f"{Colors.CYAN}{'=' * 60}{Colors.END}")
        print(report)

        return len(self.issues) == 0


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="PolarDB for PostgreSQL Daily Health Check Agent"
    )
    parser.add_argument(
        "--config",
        "-c",
        help="Path to configuration file (default: assets/db_config.env)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Path to output report file (default: polar_daily_health_report.md)",
    )

    args = parser.parse_args()

    agent = PolarDBCheckAgent(config_file=args.config, output_file=args.output)

    success = agent.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
