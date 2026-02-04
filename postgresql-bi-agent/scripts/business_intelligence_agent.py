#!/usr/bin/env python3
"""
PostgreSQL Business Intelligence Agent

A comprehensive tool for analyzing PostgreSQL databases and generating
business intelligence reports. Automatically discovers database structure,
samples data, generates business-relevant SQL queries, executes them,
and produces deep analytical reports.

Usage:
    python3 business_intelligence_agent.py --full-analysis
    python3 business_intelligence_agent.py --skill discover_database_metadata
    python3 business_intelligence_agent.py --skill generate_revenue_queries
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re
import statistics
from collections import defaultdict

# PostgreSQL connection
try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# Data analysis
try:
    import pandas as pd
    import numpy as np
except ImportError:
    print("Error: pandas/numpy not installed. Run: pip install pandas numpy")
    sys.exit(1)

# Report generation
try:
    from jinja2 import Template
except ImportError:
    print("Error: jinja2 not installed. Run: pip install jinja2")
    sys.exit(1)


class Colors:
    """ANSI color codes for terminal output."""
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    END = '\033[0m'
    BOLD = '\033[1m'


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = "digoal"
    password: str = ""
    database: str = "postgres"
    connect_timeout: int = 30


@dataclass
class TableMetadata:
    """Metadata for a database table."""
    schema_name: str
    table_name: str
    row_count: int = 0
    columns: List[Dict[str, Any]] = field(default_factory=list)
    indexes: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class BusinessQuery:
    """A generated business intelligence query."""
    name: str
    category: str
    sql: str
    description: str
    metrics: List[str] = field(default_factory=list)


class PostgreSQLBIAgent:
    """Main Business Intelligence Agent class."""
    
    def __init__(self, config: DatabaseConfig, sample_size: int = 1000, 
                 date_range_days: int = 30, output_dir: str = "output"):
        """Initialize the BI Agent."""
        self.config = config
        self.sample_size = sample_size
        self.date_range_days = date_range_days
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.connection = None
        self.metadata: Dict[str, Any] = {}
        self.query_results: Dict[str, Any] = {}
        self.business_metrics: Dict[str, Any] = {}
        self.insights: List[Dict[str, Any]] = []
        
    def connect(self) -> bool:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                connect_timeout=self.config.connect_timeout
            )
            self.connection.autocommit = True
            print(f"{Colors.GREEN}✓ Connected to {self.config.database}@"
                  f"{self.config.host}:{self.config.port}{Colors.END}")
            return True
        except Exception as e:
            print(f"{Colors.RED}✗ Connection failed: {e}{Colors.END}")
            return False
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def execute_query(self, query: str, params: tuple = None, 
                      timeout: int = 60) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries."""
        if not self.connection:
            raise Exception("Not connected to database")
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                columns = [desc[0] for desc in cursor.description]
                results = []
                for row in cursor:
                    row_dict = dict(row)
                    # Convert special types
                    for key, value in row_dict.items():
                        if isinstance(value, (datetime, timedelta)):
                            row_dict[key] = str(value)
                        elif isinstance(value, (np.integer, np.floating)):
                            row_dict[key] = float(value)
                    results.append(row_dict)
                return results
        except Exception as e:
            print(f"{Colors.YELLOW}Query error: {e}{Colors.END}")
            raise
    
    def get_table_row_count(self, schema: str, table: str) -> int:
        """Get approximate row count for a table."""
        query = f"""
            SELECT n_live_tup as approximate_count
            FROM pg_stat_user_tables
            WHERE schemaname = %s AND relname = %s
        """
        try:
            results = self.execute_query(query, (schema, table))
            if results:
                return results[0].get('approximate_count', 0)
        except:
            pass
        return 0
    
    # =========================================================================
    # METADATA DISCOVERY SKILLS
    # =========================================================================
    
    def discover_database_metadata(self) -> Dict[str, Any]:
        """Discover comprehensive database metadata."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Discovering Database Metadata{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        metadata = {
            "discovery_time": datetime.now().isoformat(),
            "database_name": self.config.database,
            "schemas": [],
            "tables": [],
            "total_tables": 0,
            "total_columns": 0,
            "relationships": []
        }
        
        # Get all schemas
        schema_query = """
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
        """
        schemas = self.execute_query(schema_query)
        metadata["schemas"] = [s["schema_name"] for s in schemas]
        
        # Get tables and columns for each schema
        tables_query = """
            SELECT 
                t.table_schema,
                t.table_name,
                t.table_type,
                c.column_name,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.ordinal_position,
                tc.constraint_type,
                kcu.column_name as pk_column
            FROM information_schema.tables t
            LEFT JOIN information_schema.columns c 
                ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            LEFT JOIN information_schema.table_constraints tc 
                ON t.table_schema = tc.table_schema AND t.table_name = tc.table_name
                AND tc.constraint_type = 'PRIMARY KEY'
            LEFT JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND c.column_name = kcu.column_name
            WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY t.table_schema, t.table_name, c.ordinal_position
        """
        
        tables_data = self.execute_query(tables_query)
        
        # Group columns by table
        table_columns = defaultdict(list)
        for row in tables_data:
            table_key = (row["table_schema"], row["table_name"])
            table_columns[table_key].append({
                "name": row["column_name"],
                "type": row["data_type"],
                "nullable": row["is_nullable"] == "YES",
                "default": row["column_default"],
                "is_pk": row["pk_column"] is not None
            })
        
        # Build table metadata
        for (schema_name, table_name), columns in table_columns.items():
            row_count = self.get_table_row_count(schema_name, table_name)
            
            # Get indexes
            indexes_query = """
                SELECT indexname 
                FROM pg_indexes 
                WHERE schemaname = %s AND tablename = %s
            """
            indexes = self.execute_query(indexes_query, (schema_name, table_name))
            index_names = [idx["indexname"] for idx in indexes]
            
            # Get foreign keys
            fk_query = """
                SELECT 
                    kcu.column_name,
                    ccu.table_schema AS foreign_schema,
                    ccu.table_name AS foreign_table,
                    ccu.column_name AS foreign_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
            """
            foreign_keys = self.execute_query(fk_query, (schema_name, table_name))
            
            table_metadata = {
                "schema_name": schema_name,
                "table_name": table_name,
                "table_type": "BASE TABLE",
                "row_count": row_count,
                "columns": columns,
                "indexes": index_names,
                "foreign_keys": [
                    {
                        "column": fk["column_name"],
                        "references_schema": fk["foreign_schema"],
                        "references_table": fk["foreign_table"],
                        "references_column": fk["foreign_column"]
                    }
                    for fk in foreign_keys
                ]
            }
            
            metadata["tables"].append(table_metadata)
            metadata["total_columns"] += len(columns)
        
        metadata["total_tables"] = len(metadata["tables"])
        
        # Analyze relationships
        self.analyze_table_relationships(metadata)
        metadata["relationships"] = self.metadata.get("relationships", [])
        
        self.metadata = metadata
        
        print(f"{Colors.GREEN}✓ Discovered {metadata['total_tables']} tables "
              f"in {len(metadata['schemas'])} schemas{Colors.END}")
        print(f"{Colors.GREEN}✓ Found {metadata['total_columns']} columns "
              f"and {len(metadata['relationships'])} relationships{Colors.END}\n")
        
        return metadata
    
    def analyze_table_relationships(self, metadata: Dict[str, Any]):
        """Analyze and map table relationships."""
        relationships = []
        
        for table in metadata.get("tables", []):
            for fk in table.get("foreign_keys", []):
                relationships.append({
                    "from_table": f"{table['schema_name']}.{table['table_name']}",
                    "from_column": fk["column"],
                    "to_table": f"{fk['references_schema']}.{fk['references_table']}",
                    "to_column": fk["references_column"],
                    "type": "FK"
                })
        
        # Infer additional relationships based on naming patterns
        table_names = {t["table_name"]: t for t in metadata.get("tables", [])}
        
        # Common patterns: user_id, order_id, product_id, etc.
        id_patterns = {
            "user": ["users", "customers", "accounts", "members"],
            "order": ["orders", "purchases", "transactions", "bookings"],
            "product": ["products", "items", "goods", "skus"],
            "category": ["categories", "types", "classifications"]
        }
        
        inferred = []
        for table in metadata.get("tables", []):
            for col in table.get("columns", []):
                col_name = col["name"].lower()
                for pattern_name, pattern_tables in id_patterns.items():
                    if col_name == f"{pattern_name}_id":
                        for target_table in pattern_tables:
                            if target_table in table_names and target_table != table["table_name"]:
                                # Check if target has matching id column
                                target_cols = [c["name"].lower() for c in 
                                              table_names[target_table].get("columns", [])]
                                if col_name in target_cols:
                                    if not any(r["from_table"] == f"{table['schema_name']}.{table['table_name']}" 
                                              and r["to_table"] == f"{table_names[target_table]['schema_name']}.{target_table}"
                                              for r in relationships):
                                        inferred.append({
                                            "from_table": f"{table['schema_name']}.{table['table_name']}",
                                            "from_column": col["name"],
                                            "to_table": f"{table_names[target_table]['schema_name']}.{target_table}",
                                            "to_column": col_name,
                                            "type": "INFERRED"
                                        })
        
        relationships.extend(inferred)
        self.metadata["relationships"] = relationships
        self.metadata["explicit_relationships"] = len([r for r in relationships if r["type"] == "FK"])
        self.metadata["inferred_relationships"] = len(inferred)
    
    def identify_business_tables(self) -> Dict[str, Any]:
        """Identify tables related to core business operations."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Identifying Business Tables{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        # Scoring system for business relevance
        business_keywords = {
            "transactions": ["order", "transaction", "payment", "purchase", "sale", "invoice", "receipt"],
            "customers": ["user", "customer", "account", "member", "client", "profile", "contact"],
            "products": ["product", "item", "sku", "goods", "merchandise", "inventory", "stock"],
            "marketing": ["campaign", "coupon", "promotion", "discount", "utm", "source", "medium"],
            "analytics": ["event", "session", "click", "view", "log", "tracking", "analytics"]
        }
        
        table_scores = []
        
        for table in self.metadata.get("tables", []):
            score = 0
            matched_categories = []
            
            table_name_lower = table["table_name"].lower()
            
            for category, keywords in business_keywords.items():
                for keyword in keywords:
                    if keyword in table_name_lower:
                        score += 1
                        if category not in matched_categories:
                            matched_categories.append(category)
            
            # Boost score for larger tables (more likely to be active business data)
            if table["row_count"] > 10000:
                score += 2
            elif table["row_count"] > 1000:
                score += 1
            
            # Boost for tables with indexes (actively used)
            if len(table.get("indexes", [])) > 3:
                score += 1
            
            if score > 0:
                table_scores.append({
                    "name": f"{table['schema_name']}.{table['table_name']}",
                    "table_name": table["table_name"],
                    "schema_name": table["schema_name"],
                    "score": score,
                    "categories": matched_categories,
                    "row_count": table["row_count"],
                    "confidence": min(score / 5.0, 1.0)
                })
        
        # Sort by score
        table_scores.sort(key=lambda x: x["score"], reverse=True)
        
        # Categorize tables
        categorized = {
            "core_tables": [],
            "transaction_tables": [],
            "customer_tables": [],
            "product_tables": [],
            "marketing_tables": [],
            "analytics_tables": []
        }
        
        for table in table_scores:
            for category in table["categories"]:
                if category in categorized:
                    categorized[category + "_tables"].append({
                        "name": table["name"],
                        "confidence": table["confidence"],
                        "row_count": table["row_count"]
                    })
        
        # Top core tables
        categorized["core_tables"] = [
            {
                "name": t["name"],
                "table_name": t["table_name"],
                "category": t["categories"][0] if t["categories"] else "unknown",
                "confidence": t["confidence"],
                "row_count": t["row_count"]
            }
            for t in table_scores[:10]
        ]
        
        result = {
            "total_business_tables": len(table_scores),
            **categorized
        }
        
        self.metadata["business_tables"] = result
        
        print(f"{Colors.GREEN}✓ Identified {len(table_scores)} business-relevant tables{Colors.END}")
        print(f"  - Core tables: {len(categorized['core_tables'])}")
        print(f"  - Transaction tables: {len(categorized['transaction_tables'])}")
        print(f"  - Customer tables: {len(categorized['customer_tables'])}")
        print(f"  - Product tables: {len(categorized['product_tables'])}")
        print(f"  - Marketing tables: {len(categorized['marketing_tables'])}")
        print(f"  - Analytics tables: {len(categorized['analytics_tables'])}\n")
        
        return result
    
    # =========================================================================
    # DATA SAMPLING & PATTERN ANALYSIS
    # =========================================================================
    
    def sample_table_data(self, tables: List[str] = None) -> Dict[str, Any]:
        """Sample data from business tables."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Sampling Table Data{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        if not tables:
            # Use top business tables
            tables = [t["name"] for t in self.metadata.get("business_tables", {}).get("core_tables", [])[:5]]
        
        sampled_data = {}
        
        for table_full in tables:
            parts = table_full.split(".")
            schema = parts[0] if len(parts) > 1 else "public"
            table = parts[-1]
            
            # Get sample data
            sample_query = f"""
                SELECT * FROM {schema}.{table}
                LIMIT %s
            """
            try:
                samples = self.execute_query(sample_query, (self.sample_size,))
                
                if not samples:
                    continue
                
                # Analyze sample
                analysis = {
                    "sample_size": len(samples),
                    "columns_analyzed": len(samples[0]) if samples else 0,
                    "date_ranges": {},
                    "value_ranges": {},
                    "categorical_distributions": {},
                    "null_counts": {}
                }
                
                # Get row count
                count_query = f"SELECT COUNT(*) as cnt FROM {schema}.{table}"
                count_result = self.execute_query(count_query)
                analysis["total_rows"] = count_result[0].get("cnt", 0) if count_result else 0
                
                # Analyze each column
                for col_name, value in samples[0].items():
                    null_count = sum(1 for row in samples if row.get(col_name) is None)
                    analysis["null_counts"][col_name] = null_count
                    
                    # Detect data type and analyze
                    values = [row.get(col_name) for row in samples if row.get(col_name) is not None]
                    
                    if not values:
                        continue
                    
                    # Check for date/time columns
                    if isinstance(values[0], str):
                        try:
                            parsed_dates = [datetime.fromisoformat(v.replace('Z', '+00:00')) 
                                          for v in values[:100] if 'T' in v or '-' in v]
                            if len(parsed_dates) > len(values) * 0.5:
                                date_values = [datetime.fromisoformat(str(v)) for v in values if v]
                                analysis["date_ranges"][col_name] = {
                                    "min": str(min(date_values)) if date_values else None,
                                    "max": str(max(date_values)) if date_values else None
                                }
                                continue
                        except:
                            pass
                    
                    # Numeric analysis
                    numeric_values = [float(v) for v in values if self._is_numeric(v)]
                    if len(numeric_values) > len(values) * 0.5:
                        analysis["value_ranges"][col_name] = {
                            "min": min(numeric_values),
                            "max": max(numeric_values),
                            "avg": sum(numeric_values) / len(numeric_values),
                            "count": len(numeric_values)
                        }
                    
                    # Categorical analysis
                    if len(set(values)) <= 20:  # Low cardinality
                        value_counts = {}
                        for v in values:
                            v_str = str(v)
                            value_counts[v_str] = value_counts.get(v_str, 0) + 1
                        if len(value_counts) <= 20:
                            analysis["categorical_distributions"][col_name] = value_counts
                
                sampled_data[table_full] = analysis
                
            except Exception as e:
                print(f"{Colors.YELLOW}  Warning: Could not sample {table_full}: {e}{Colors.END}")
                continue
        
        self.metadata["sampled_data"] = sampled_data
        
        print(f"{Colors.GREEN}✓ Sampled {len(sampled_data)} tables{Colors.END}\n")
        return sampled_data
    
    def _is_numeric(self, value) -> bool:
        """Check if a value is numeric."""
        if value is None:
            return False
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def detect_data_patterns(self) -> Dict[str, Any]:
        """Detect temporal and value patterns in the data."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Detecting Data Patterns{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        patterns = {
            "temporal_patterns": {},
            "value_distributions": {},
            "data_quality": {}
        }
        
        # Analyze temporal patterns
        sampled_data = self.metadata.get("sampled_data", {})
        
        for table, data in sampled_data.items():
            if "created_at" in data.get("date_ranges", {}):
                date_range = data["date_ranges"]["created_at"]
                patterns["temporal_patterns"][table] = {
                    "date_column": "created_at",
                    "date_range": date_range,
                    "age_days": (datetime.now() - datetime.fromisoformat(date_range["min"])).days 
                               if date_range.get("min") else None
                }
        
        # Detect value distributions and outliers
        for table, data in sampled_data.items():
            for col, range_info in data.get("value_ranges", {}).items():
                if "order_amount" in col.lower() or "amount" in col.lower():
                    values = list(range_info.values())
                    if "avg" in range_info:
                        distribution = {
                            "column": col,
                            "mean": range_info["avg"],
                            "min": range_info["min"],
                            "max": range_info["max"],
                            "percentiles": {}
                        }
                        # Calculate percentiles from sample
                        if "count" in range_info and range_info["count"] > 10:
                            try:
                                percentiles_query = f"""
                                    SELECT 
                                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {col}) as p25,
                                        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {col}) as p50,
                                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {col}) as p75,
                                        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {col}) as p95
                                    FROM {table}
                                    WHERE {col} IS NOT NULL
                                """
                                pct_results = self.execute_query(percentiles_query)
                                if pct_results:
                                    distribution["percentiles"] = {
                                        "25": pct_results[0].get("p25"),
                                        "50": pct_results[0].get("p50"),
                                        "75": pct_results[0].get("p75"),
                                        "95": pct_results[0].get("p95")
                                    }
                            except:
                                pass
                        
                        patterns["value_distributions"][f"{table}.{col}"] = distribution
        
        # Data quality checks
        for table, data in sampled_data.items():
            null_rates = {}
            for col, null_count in data.get("null_counts", {}).items():
                null_rate = null_count / data["sample_size"]
                if null_rate > 0.1:  # More than 10% null
                    null_rates[col] = round(null_rate * 100, 2)
            
            if null_rates:
                patterns["data_quality"][table] = {
                    "null_rates": null_rates,
                    "completeness_score": 100 - sum(null_rates.values()) / len(null_rates)
                }
        
        self.metadata["patterns"] = patterns
        
        print(f"{Colors.GREEN}✓ Detected patterns in {len(patterns['temporal_patterns'])} temporal series{Colors.END}")
        print(f"{Colors.GREEN}✓ Analyzed {len(patterns['value_distributions'])} value distributions{Colors.END}")
        print(f"{Colors.GREEN}✓ Checked data quality for {len(patterns['data_quality'])} tables{Colors.END}\n")
        
        return patterns
    
    def infer_business_context(self) -> Dict[str, Any]:
        """Infer business domain and context from metadata."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Inferring Business Context{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        context = {
            "business_domain": None,
            "business_model": None,
            "primary_entities": [],
            "secondary_entities": [],
            "key_metrics": [],
            "time_granularities": ["daily", "weekly", "monthly", "quarterly"]
        }
        
        # Detect business domain based on table names
        business_domains = {
            "E-commerce": ["order", "product", "cart", "checkout", "inventory", "sku", "shipping"],
            "SaaS": ["subscription", "tenant", "license", "plan", "feature", "usage", "metric"],
            "Finance": ["transaction", "account", "balance", "transfer", "payment", "invoice"],
            "Social": ["post", "comment", "like", "follow", "connection", "message"],
            "Logistics": ["shipment", "delivery", "route", "driver", "vehicle", "warehouse"]
        }
        
        tables = self.metadata.get("business_tables", {}).get("core_tables", [])
        table_names_lower = " ".join([t.get("table_name", "").lower() for t in tables])
        
        for domain, keywords in business_domains.items():
            match_count = sum(1 for kw in keywords if kw.lower() in table_names_lower)
            if match_count >= 3:
                context["business_domain"] = domain
                break
        
        if not context["business_domain"]:
            # Check for common business patterns
            if any(t["table_name"].lower() in ["orders", "products", "customers"] for t in tables):
                context["business_domain"] = "E-commerce Retail"
            elif any(t["table_name"].lower() in ["users", "accounts", "transactions"] for t in tables):
                context["business_domain"] = "General Business"
        
        # Detect business model
        if "subscription" in table_names_lower or "plan" in table_names_lower:
            context["business_model"] = "B2B SaaS"
        elif "cart" in table_names_lower or "checkout" in table_names_lower:
            context["business_model"] = "B2C E-commerce"
        else:
            context["business_model"] = "B2B/B2C Hybrid"
        
        # Identify primary entities
        core_entities = {
            "Orders/Transactions": ["order", "transaction", "purchase", "booking"],
            "Customers/Users": ["user", "customer", "account", "member"],
            "Products/Items": ["product", "item", "sku", "goods"],
            "Payments": ["payment", "invoice", "billing"],
            "Marketing": ["campaign", "coupon", "promotion"]
        }
        
        for entity, keywords in core_entities.items():
            for keyword in keywords:
                if keyword in table_names_lower:
                    if entity not in context["primary_entities"]:
                        context["primary_entities"].append(entity)
                    break
        
        # Define key metrics based on domain
        context["key_metrics"] = [
            "Total Revenue/GMV",
            "Average Order Value",
            "Number of Orders",
            "Customer Acquisition Cost",
            "Customer Lifetime Value",
            "Conversion Rate",
            "Repeat Purchase Rate",
            "Customer Retention Rate"
        ]
        
        # Add domain-specific metrics
        if context["business_domain"] == "E-commerce Retail":
            context["key_metrics"].extend([
                "Cart Abandonment Rate",
                "Inventory Turnover",
                "Return Rate"
            ])
        elif context["business_domain"] == "SaaS":
            context["key_metrics"].extend([
                "Monthly Recurring Revenue (MRR)",
                "Churn Rate",
                "Net Revenue Retention"
            ])
        
        self.metadata["business_context"] = context
        
        print(f"{Colors.GREEN}✓ Business Domain: {context['business_domain']}{Colors.END}")
        print(f"{Colors.GREEN}✓ Business Model: {context['business_model']}{Colors.END}")
        print(f"{Colors.GREEN}✓ Primary Entities: {', '.join(context['primary_entities'])}{Colors.END}")
        print(f"{Colors.GREEN}✓ Key Metrics: {len(context['key_metrics'])} defined{Colors.END}\n")
        
        return context
    
    # =========================================================================
    # BUSINESS INTELLIGENCE SQL GENERATION
    # =========================================================================
    
    def _get_date_filter(self, table_alias: str = "") -> str:
        """Generate date filter SQL for the analysis period."""
        prefix = f"{table_alias}." if table_alias else ""
        return f"""
            WHERE {prefix}created_at >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
            OR {prefix}order_date >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
        """
    
    def _get_date_column(self, table: str) -> str:
        """Detect the date column for a table."""
        date_columns = ["created_at", "order_date", "updated_at", "transaction_date"]
        
        for table_meta in self.metadata.get("tables", []):
            if table_meta["table_name"] == table:
                for col in table_meta.get("columns", []):
                    if col["name"] in date_columns:
                        return col["name"]
        
        return "created_at"
    
    def generate_revenue_queries(self) -> List[BusinessQuery]:
        """Generate SQL queries for revenue analysis."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Generating Revenue Queries{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        queries = []
        
        # Find relevant tables
        orders_table = self._find_table(["orders", "purchases", "transactions"])
        date_col = self._get_date_column(orders_table or "orders")
        
        if orders_table:
            # Daily Revenue Trend
            queries.append(BusinessQuery(
                name="daily_revenue_trend",
                category="Revenue",
                sql=f"""
                    SELECT 
                        DATE({date_col}) as date,
                        SUM(order_amount) as revenue,
                        COUNT(*) as order_count,
                        ROUND(AVG(order_amount)::numeric, 2) as avg_order_value
                    FROM {orders_table}
                    WHERE {date_col} >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                    GROUP BY DATE({date_col})
                    ORDER BY date
                """,
                description="Daily revenue, order count, and AOV for the analysis period",
                metrics=["daily_revenue", "daily_orders", "aov", "revenue_trend"]
            ))
            
            # Revenue by Category
            category_table = self._find_table(["categories", "category"])
            product_table = self._find_table(["products", "items"])
            
            if category_table and product_table:
                queries.append(BusinessQuery(
                    name="revenue_by_category",
                    category="Revenue",
                    sql=f"""
                        SELECT 
                            c.name as category,
                            SUM(oi.quantity * oi.unit_price) as revenue,
                            COUNT(DISTINCT o.order_id) as orders,
                            ROUND(AVG(oi.quantity * oi.unit_price)::numeric, 2) as avg_item_value,
                            COUNT(DISTINCT o.user_id) as unique_buyers
                        FROM {product_table} p
                        JOIN {category_table} c ON p.category_id = c.id
                        JOIN order_items oi ON p.id = oi.product_id
                        JOIN {orders_table} o ON oi.order_id = o.id
                        WHERE o.{date_col} >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                        GROUP BY c.name
                        ORDER BY revenue DESC
                        LIMIT 20
                    """,
                    description="Revenue breakdown by product category",
                    metrics=["category_revenue", "category_orders", "category_aov"]
                ))
            
            # Monthly Revenue Comparison
            queries.append(BusinessQuery(
                name="monthly_revenue_comparison",
                category="Revenue",
                sql=f"""
                    SELECT 
                        DATE_TRUNC('month', {date_col}) as month,
                        SUM(order_amount) as revenue,
                        COUNT(*) as orders,
                        ROUND(AVG(order_amount)::numeric, 2) as avg_order_value
                    FROM {orders_table}
                    WHERE {date_col} >= CURRENT_DATE - INTERVAL '12 months'
                    GROUP BY DATE_TRUNC('month', {date_col})
                    ORDER BY month
                """,
                description="Monthly revenue comparison for YoY analysis",
                metrics=["monthly_revenue", "mom_growth", "yoy_growth"]
            ))
            
            # Payment Method Breakdown
            payment_table = self._find_table(["payments", "payment"])
            if payment_table:
                queries.append(BusinessQuery(
                    name="revenue_by_payment_method",
                    category="Revenue",
                    sql=f"""
                        SELECT 
                            COALESCE(payment_method, 'unknown') as method,
                            COUNT(*) as transaction_count,
                            SUM(amount) as total_amount,
                            ROUND(AVG(amount)::numeric, 2) as avg_amount,
                            ROUND((COUNT(CASE WHEN status = 'completed' THEN 1 END)::numeric / NULLIF(COUNT(*), 0) * 100), 2) as success_rate
                        FROM {payment_table}
                        WHERE created_at >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                        GROUP BY payment_method
                        ORDER BY total_amount DESC
                    """,
                    description="Revenue breakdown by payment method",
                    metrics=["payment_revenue", "payment_count", "payment_success_rate"]
                ))
        
        print(f"{Colors.GREEN}✓ Generated {len(queries)} revenue queries{Colors.END}\n")
        return queries
    
    def generate_customer_analytics_queries(self) -> List[BusinessQuery]:
        """Generate SQL for customer analytics."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Generating Customer Analytics Queries{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        queries = []
        
        users_table = self._find_table(["users", "customers", "accounts"])
        orders_table = self._find_table(["orders", "purchases", "transactions"])
        
        if users_table:
            date_col = self._get_date_column(users_table)
            
            # Customer Acquisition Trend
            queries.append(BusinessQuery(
                name="customer_acquisition_trend",
                category="Customer",
                sql=f"""
                    SELECT 
                        DATE_TRUNC('week', {date_col}) as week,
                        COUNT(*) as new_customers,
                        COUNT(DISTINCT CASE WHEN source IN ('paid', 'organic') THEN id END) as acquired_customers
                    FROM {users_table}
                    WHERE {date_col} >= CURRENT_DATE - INTERVAL '12 weeks'
                    GROUP BY DATE_TRUNC('week', {date_col})
                    ORDER BY week
                """,
                description="Weekly new customer acquisition trend",
                metrics=["new_users", "user_growth_rate", "acquisition_sources"]
            ))
            
            # Customer Segmentation by Value
            if orders_table:
                queries.append(BusinessQuery(
                    name="customer_segmentation",
                    category="Customer",
                    sql=f"""
                        SELECT 
                            CASE 
                                WHEN total_spent < 100 THEN 'Low Value'
                                WHEN total_spent < 500 THEN 'Medium Value'
                                WHEN total_spent < 2000 THEN 'High Value'
                                ELSE 'Premium'
                            END as segment,
                            COUNT(*) as customer_count,
                            ROUND(AVG(order_count)::numeric, 1) as avg_orders,
                            ROUND(AVG(total_spent)::numeric, 2) as avg_ltv,
                            ROUND(AVG(days_since_first_purchase)::numeric, 0) as avg_customer_age
                        FROM (
                            SELECT 
                                u.id,
                                u.{date_col} as first_purchase,
                                CURRENT_DATE - DATE(u.{date_col}) as days_since_first_purchase,
                                COALESCE(SUM(o.order_amount), 0) as total_spent,
                                COUNT(o.id) as order_count
                            FROM {users_table} u
                            LEFT JOIN {orders_table} o ON u.id = o.user_id
                            GROUP BY u.id, u.{date_col}
                        ) t
                        GROUP BY segment
                        ORDER BY avg_ltv DESC
                    """,
                    description="Customer segmentation by lifetime value and behavior",
                    metrics=["segment_distribution", "segment_ltv", "segment_behavior"]
                ))
                
                # Customer Retention Cohort Analysis
                queries.append(BusinessQuery(
                    name="customer_retention_cohort",
                    category="Customer",
                    sql=f"""
                        WITH cohorts AS (
                            SELECT 
                                user_id,
                                DATE_TRUNC('month', first_purchase) as cohort_month,
                                COUNT(DISTINCT DATE_TRUNC('month', order_date)) as active_months,
                                COUNT(DISTINCT order_id) as total_orders
                            FROM (
                                SELECT 
                                    user_id,
                                    MIN(created_at) as first_purchase,
                                    DATE_TRUNC('month', created_at) as order_date,
                                    id as order_id
                                FROM {orders_table}
                                GROUP BY user_id, id
                            ) o
                            GROUP BY user_id, DATE_TRUNC('month', first_purchase)
                        )
                        SELECT 
                            cohort_month,
                            COUNT(*) as cohort_size,
                            ROUND(AVG(active_months)::numeric, 1) as avg_active_months,
                            ROUND(AVG(total_orders)::numeric, 1) as avg_orders,
                            ROUND(AVG(total_orders)::numeric / NULLIF(MAX(active_months), 0), 2) as orders_per_month
                        FROM cohorts
                        GROUP BY cohort_month
                        ORDER BY cohort_month
                    """,
                    description="Customer retention by cohort month",
                    metrics=["cohort_retention", "cohort_size", "customer_lifespan"]
                ))
                
                # Customer Activity Levels
                queries.append(BusinessQuery(
                    name="customer_activity_levels",
                    category="Customer",
                    sql=f"""
                        SELECT 
                            activity_level,
                            COUNT(*) as customer_count,
                            ROUND((COUNT(*)::numeric / SUM(COUNT(*)) OVER()) * 100, 2) as percentage,
                            ROUND(AVG(total_spent)::numeric, 2) as avg_spent
                        FROM (
                            SELECT 
                                user_id,
                                COUNT(*) as order_count,
                                SUM(order_amount) as total_spent,
                                CASE 
                                    WHEN COUNT(*) = 1 THEN 'One-time'
                                    WHEN COUNT(*) BETWEEN 2 AND 5 THEN 'Regular'
                                    WHEN COUNT(*) BETWEEN 6 AND 12 THEN 'Frequent'
                                    ELSE 'Loyal'
                                END as activity_level
                            FROM {orders_table}
                            WHERE created_at >= CURRENT_DATE - INTERVAL '12 months'
                            GROUP BY user_id
                        ) t
                        GROUP BY activity_level
                        ORDER BY avg_spent DESC
                    """,
                    description="Customer activity level distribution",
                    metrics=["activity_distribution", "customer_value", "retention"]
                ))
        
        print(f"{Colors.GREEN}✓ Generated {len(queries)} customer analytics queries{Colors.END}\n")
        return queries
    
    def generate_product_analytics_queries(self) -> List[BusinessQuery]:
        """Generate SQL for product/inventory analytics."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Generating Product Analytics Queries{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        queries = []
        
        products_table = self._find_table(["products", "items", "skus"])
        orders_table = self._find_table(["orders", "purchases"])
        inventory_table = self._find_table(["inventory", "stocks"])
        
        if products_table:
            # Product Sales Ranking
            queries.append(BusinessQuery(
                name="product_sales_ranking",
                category="Product",
                sql=f"""
                    SELECT 
                        p.id,
                        p.name,
                        COALESCE(SUM(oi.quantity), 0) as total_units_sold,
                        COALESCE(SUM(oi.quantity * oi.unit_price), 0) as total_revenue,
                        COUNT(DISTINCT o.id) as order_count,
                        ROUND(COALESCE(SUM(oi.quantity * oi.unit_price), 0) / NULLIF(COUNT(DISTINCT o.id), 0), 2) as aov
                    FROM {products_table} p
                    LEFT JOIN order_items oi ON p.id = oi.product_id
                    LEFT JOIN {orders_table} o ON oi.order_id = o.id AND o.created_at >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                    GROUP BY p.id, p.name
                    ORDER BY total_revenue DESC
                    LIMIT 20
                """,
                description="Top 20 products by revenue",
                metrics=["product_revenue", "units_sold", "product_popularity"]
            ))
            
            # Category Performance
            category_table = self._find_table(["categories", "types"])
            if category_table:
                queries.append(BusinessQuery(
                    name="category_performance",
                    category="Product",
                    sql=f"""
                        SELECT 
                            c.name as category,
                            COUNT(DISTINCT p.id) as product_count,
                            COALESCE(SUM(oi.quantity), 0) as total_units,
                            COALESCE(SUM(oi.quantity * oi.unit_price), 0) as revenue,
                            ROUND(COALESCE(SUM(oi.quantity * oi.unit_price), 0) / NULLIF(COUNT(DISTINCT o.id), 0), 2) as avg_order_value,
                            ROUND(COALESCE(SUM(oi.quantity), 0)::numeric / NULLIF(COUNT(DISTINCT p.id), 0), 1) as avg_units_per_product
                        FROM {category_table} c
                        LEFT JOIN {products_table} p ON c.id = p.category_id
                        LEFT JOIN order_items oi ON p.id = oi.product_id
                        LEFT JOIN {orders_table} o ON oi.order_id = o.id AND o.created_at >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                        GROUP BY c.name
                        ORDER BY revenue DESC
                    """,
                    description="Performance metrics by product category",
                    metrics=["category_revenue", "category_growth", "category_margin"]
                ))
            
            # Inventory Turnover
            if inventory_table:
                queries.append(BusinessQuery(
                    name="inventory_turnover",
                    category="Product",
                    sql=f"""
                        SELECT 
                            p.id,
                            p.name,
                            COALESCE(i.quantity, 0) as stock_quantity,
                            COALESCE(SUM(oi.quantity), 0) as sales_last_30d,
                            CASE 
                                WHEN COALESCE(i.quantity, 0) > 0 
                                THEN ROUND((COALESCE(SUM(oi.quantity), 0)::decimal / NULLIF(i.quantity, 0)) * 30, 2)
                                ELSE 0 
                            END as daily_turnover_rate,
                            CASE 
                                WHEN COALESCE(SUM(oi.quantity), 0) > COALESCE(i.quantity, 0) * 0.2 
                                THEN 'Low Stock'
                                WHEN COALESCE(SUM(oi.quantity), 0) > COALESCE(i.quantity, 0) * 0.1 
                                THEN 'Medium Stock'
                                ELSE 'Healthy Stock'
                            END as stock_status
                        FROM {products_table} p
                        LEFT JOIN {inventory_table} i ON p.id = i.product_id
                        LEFT JOIN order_items oi ON p.id = oi.product_id
                        LEFT JOIN {orders_table} o ON oi.order_id = o.id AND o.created_at >= CURRENT_DATE - INTERVAL '30 days'
                        GROUP BY p.id, p.name, i.quantity
                        ORDER BY daily_turnover_rate DESC
                    """,
                    description="Inventory turnover rates and stock status",
                    metrics=["turnover_rate", "stock_velocity", "reorder_point"]
                ))
        
        print(f"{Colors.GREEN}✓ Generated {len(queries)} product analytics queries{Colors.END}\n")
        return queries
    
    def generate_operational_analytics_queries(self) -> List[BusinessQuery]:
        """Generate SQL for operational metrics."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Generating Operational Analytics Queries{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        queries = []
        
        orders_table = self._find_table(["orders", "purchases"])
        payments_table = self._find_table(["payments"])
        
        if orders_table:
            # Order Fulfillment Time
            queries.append(BusinessQuery(
                name="order_fulfillment_time",
                category="Operations",
                sql=f"""
                    SELECT 
                        DATE(created_at) as date,
                        COUNT(*) as total_orders,
                        COUNT(CASE WHEN shipped_at IS NOT NULL THEN 1 END) as fulfilled_orders,
                        ROUND(AVG(EXTRACT(EPOCH FROM (shipped_at - created_at)) / 3600)::numeric, 1) as avg_fulfillment_hours,
                        ROUND((COUNT(CASE WHEN shipped_at IS NOT NULL THEN 1 END)::numeric / NULLIF(COUNT(*), 0) * 100), 2) as fulfillment_rate
                    FROM {orders_table}
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                    GROUP BY DATE(created_at)
                    ORDER BY date
                """,
                description="Order fulfillment efficiency over time",
                metrics=["avg_fulfillment_time", "fulfillment_rate", "orders_fulfilled"]
            ))
            
            # Order Status Distribution
            queries.append(BusinessQuery(
                name="order_status_distribution",
                category="Operations",
                sql=f"""
                    SELECT 
                        status,
                        COUNT(*) as order_count,
                        ROUND((COUNT(*)::numeric / (SELECT COUNT(*) FROM {orders_table} WHERE created_at >= CURRENT_DATE - INTERVAL '{self.date_range_days} days')) * 100, 2) as percentage,
                        ROUND(AVG(order_amount)::numeric, 2) as avg_amount
                    FROM {orders_table}
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                    GROUP BY status
                    ORDER BY order_count DESC
                """,
                description="Order status breakdown",
                metrics=["status_distribution", "status_trends"]
            ))
            
            # Return Rate Analysis
            queries.append(BusinessQuery(
                name="return_rate_analysis",
                category="Operations",
                sql=f"""
                    SELECT 
                        DATE_TRUNC('week', created_at) as week,
                        COUNT(*) as total_orders,
                        COUNT(CASE WHEN status = 'returned' THEN 1 END) as returned_orders,
                        ROUND((COUNT(CASE WHEN status = 'returned' THEN 1 END)::numeric / NULLIF(COUNT(*), 0) * 100), 2) as return_rate,
                        ROUND(AVG(CASE WHEN status = 'returned' THEN order_amount END)::numeric, 2) as avg_return_value
                    FROM {orders_table}
                    WHERE created_at >= CURRENT_DATE - INTERVAL '12 weeks'
                    GROUP BY DATE_TRUNC('week', created_at)
                    ORDER BY week
                """,
                description="Weekly return rate tracking",
                metrics=["weekly_return_rate", "return_trend", "return_value"]
            ))
        
        # Payment Success Rate
        if payments_table:
            queries.append(BusinessQuery(
                name="payment_success_rate",
                category="Operations",
                sql=f"""
                    SELECT 
                        DATE(created_at) as date,
                        COUNT(*) as total_transactions,
                        COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_payments,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_payments,
                        ROUND((COUNT(CASE WHEN status = 'completed' THEN 1 END)::numeric / NULLIF(COUNT(*), 0) * 100), 2) as success_rate,
                        ROUND(AVG(CASE WHEN status = 'completed' THEN amount END)::numeric, 2) as avg_payment_value
                    FROM {payments_table}
                    WHERE created_at >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                    GROUP BY DATE(created_at)
                    ORDER BY date
                """,
                description="Daily payment success rates",
                metrics=["payment_success_rate", "payment_failures", "payment_value"]
            ))
        
        print(f"{Colors.GREEN}✓ Generated {len(queries)} operational analytics queries{Colors.END}\n")
        return queries
    
    def generate_marketing_analytics_queries(self) -> List[BusinessQuery]:
        """Generate SQL for marketing analytics."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Generating Marketing Analytics Queries{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        queries = []
        
        users_table = self._find_table(["users", "customers"])
        orders_table = self._find_table(["orders", "purchases"])
        
        if users_table:
            date_col = self._get_date_column(users_table)
            
            # Channel Attribution
            queries.append(BusinessQuery(
                name="channel_attribution",
                category="Marketing",
                sql=f"""
                    SELECT 
                        COALESCE(source, 'direct') as channel,
                        COUNT(DISTINCT user_id) as total_users,
                        COUNT(DISTINCT CASE WHEN has_order THEN user_id END) as converters,
                        ROUND((COUNT(DISTINCT CASE WHEN has_order THEN user_id END)::numeric / NULLIF(COUNT(DISTINCT user_id), 0) * 100), 2) as conversion_rate,
                        ROUND(AVG(CASE WHEN total_revenue > 0 THEN total_revenue END)::numeric, 2) as avg_revenue_per_user
                    FROM (
                        SELECT 
                            u.id as user_id,
                            MAX(u.source) as source,
                            MAX(CASE WHEN o.id IS NOT NULL THEN true ELSE false END) as has_order,
                            SUM(COALESCE(o.order_amount, 0)) as total_revenue
                        FROM {users_table} u
                        LEFT JOIN {orders_table} o ON u.id = o.user_id
                        WHERE u.{date_col} >= CURRENT_DATE - INTERVAL '{self.date_range_days} days'
                        GROUP BY u.id
                    ) t
                    GROUP BY channel
                    ORDER BY converters DESC
                """,
                description="User acquisition and conversion by channel",
                metrics=["channel_users", "channel_conversion", "channel_value"]
            ))
            
            # Conversion Funnel
            queries.append(BusinessQuery(
                name="conversion_funnel",
                category="Marketing",
                sql=f"""
                    WITH funnel AS (
                        SELECT 
                            'Visit' as stage,
                            COUNT(DISTINCT session_id) as count,
                            100.0 as percentage
                        FROM events 
                        WHERE event_type = 'page_view'
                        
                        UNION ALL
                        
                        SELECT 
                            'Product View' as stage,
                            COUNT(DISTINCT session_id) as count,
                            ROUND((COUNT(DISTINCT session_id)::numeric / NULLIF(
                                (SELECT COUNT(DISTINCT session_id) FROM events WHERE event_type = 'page_view'), 0
                            ) * 100), 2) as percentage
                        FROM events 
                        WHERE event_type = 'product_view'
                        
                        UNION ALL
                        
                        SELECT 
                            'Add to Cart' as stage,
                            COUNT(DISTINCT session_id) as count,
                            ROUND((COUNT(DISTINCT session_id)::numeric / NULLIF(
                                (SELECT COUNT(DISTINCT session_id) FROM events WHERE event_type = 'product_view'), 0
                            ) * 100), 2) as percentage
                        FROM events 
                        WHERE event_type = 'add_to_cart'
                        
                        UNION ALL
                        
                        SELECT 
                            'Checkout' as stage,
                            COUNT(DISTINCT session_id) as count,
                            ROUND((COUNT(DISTINCT session_id)::numeric / NULLIF(
                                (SELECT COUNT(DISTINCT session_id) FROM events WHERE event_type = 'add_to_cart'), 0
                            ) * 100), 2) as percentage
                        FROM events 
                        WHERE event_type = 'checkout_start'
                        
                        UNION ALL
                        
                        SELECT 
                            'Purchase' as stage,
                            COUNT(DISTINCT session_id) as count,
                            ROUND((COUNT(DISTINCT session_id)::numeric / NULLIF(
                                (SELECT COUNT(DISTINCT session_id) FROM events WHERE event_type = 'checkout_start'), 0
                            ) * 100), 2) as percentage
                        FROM events 
                        WHERE event_type = 'purchase'
                    )
                    SELECT * FROM funnel
                    ORDER BY count DESC
                """,
                description="User conversion funnel from visit to purchase",
                metrics=["funnel_conversion", "drop_off_rates", "stage_progression"]
            ))
            
            # Coupon/Promotion Effectiveness
            coupon_table = self._find_table(["coupons", "promotions"])
            if coupon_table:
                queries.append(BusinessQuery(
                    name="coupon_effectiveness",
                    category="Marketing",
                    sql=f"""
                        SELECT 
                            c.code,
                            COUNT(DISTINCT o.id) as orders_with_coupon,
                            ROUND(SUM(o.order_amount)::numeric, 2) as total_revenue,
                            ROUND(AVG(o.order_amount)::numeric, 2) as avg_order_value,
                            COUNT(DISTINCT u.id) as unique_users,
                            ROUND((COUNT(DISTINCT CASE WHEN o.created_at >= c.created_at AND o.created_at <= c.created_at + INTERVAL '7 days' THEN o.id END)::numeric / NULLIF(COUNT(DISTINCT o.id), 0) * 100), 2) as redemption_rate
                        FROM {coupon_table} c
                        LEFT JOIN {orders_table} o ON o.coupon_code = c.code
                        LEFT JOIN {users_table} u ON o.user_id = u.id
                        WHERE c.created_at >= CURRENT_DATE - INTERVAL '30 days'
                        GROUP BY c.code
                        ORDER BY total_revenue DESC
                        LIMIT 10
                    """,
                    description="Top performing coupons and promotions",
                    metrics=["coupon_revenue", "coupon_orders", "coupon_redemption"]
                ))
        
        print(f"{Colors.GREEN}✓ Generated {len(queries)} marketing analytics queries{Colors.END}\n")
        return queries
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def _find_table(self, possible_names: List[str]) -> str:
        """Find a table by possible names."""
        for name in possible_names:
            for table in self.metadata.get("tables", []):
                if table["table_name"].lower() == name:
                    return f"{table['schema_name']}.{table['table_name']}"
        return None
    
    def execute_all_queries(self, query_lists: Dict[str, List[BusinessQuery]]) -> Dict[str, Any]:
        """Execute all generated queries and collect results."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Executing Business Intelligence Queries{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        execution_summary = {
            "total_queries": 0,
            "successful": 0,
            "failed": 0,
            "total_execution_time": 0,
            "errors": []
        }
        
        results = {}
        
        for category, queries in query_lists.items():
            category_results = {}
            
            for query in queries:
                execution_summary["total_queries"] += 1
                
                try:
                    import time
                    start_time = time.time()
                    
                    query_results = self.execute_query(query.sql)
                    
                    execution_time = time.time() - start_time
                    execution_summary["total_execution_time"] += execution_time
                    execution_summary["successful"] += 1
                    
                    category_results[query.name] = {
                        "description": query.description,
                        "metrics": query.metrics,
                        "execution_time": round(execution_time, 3),
                        "row_count": len(query_results),
                        "data": query_results[:1000]  # Limit result size
                    }
                    
                except Exception as e:
                    execution_summary["failed"] += 1
                    execution_summary["errors"].append({
                        "query": query.name,
                        "error": str(e)
                    })
                    
                    category_results[query.name] = {
                        "description": query.description,
                        "error": str(e),
                        "status": "failed"
                    }
            
            results[category] = category_results
        
        self.query_results = results
        execution_summary["avg_query_time"] = round(
            execution_summary["total_execution_time"] / execution_summary["total_queries"], 3
        ) if execution_summary["total_queries"] > 0 else 0
        
        self.metadata["execution_summary"] = execution_summary
        
        print(f"{Colors.GREEN}✓ Executed {execution_summary['total_queries']} queries "
              f"({execution_summary['successful']} successful, {execution_summary['failed']} failed){Colors.END}")
        print(f"{Colors.GREEN}✓ Total execution time: {execution_summary['total_execution_time']:.2f}s{Colors.END}\n")
        
        return results
    
    def calculate_business_metrics(self) -> Dict[str, Any]:
        """Calculate key business metrics from query results."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Calculating Business Metrics{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        metrics = {
            "kpis": {},
            "ratios": {},
            "trends": {}
        }
        
        # Extract KPIs from revenue queries
        if "Revenue" in self.query_results:
            daily_revenue = self.query_results["Revenue"].get("daily_revenue_trend", {}).get("data", [])
            if daily_revenue:
                revenues = [r.get("revenue", 0) for r in daily_revenue if r.get("revenue")]
                orders = [r.get("order_count", 0) for r in daily_revenue if r.get("order_count")]
                
                if revenues:
                    total_revenue = sum(revenues)
                    avg_daily_revenue = statistics.mean(revenues)
                    
                    metrics["kpis"]["total_revenue"] = round(total_revenue, 2)
                    metrics["kpis"]["avg_daily_revenue"] = round(avg_daily_revenue, 2)
                    
                    # Calculate growth
                    if len(revenues) >= 7:
                        recent_week = sum(revenues[-7:])
                        prev_week = sum(revenues[-14:-7]) if len(revenues) >= 14 else 0
                        if prev_week > 0:
                            metrics["kpis"]["revenue_growth_wo_w"] = round(
                                (recent_week - prev_week) / prev_week * 100, 2
                            )
                
                if orders:
                    metrics["kpis"]["total_orders"] = sum(orders)
                    metrics["kpis"]["avg_daily_orders"] = round(statistics.mean(orders), 1)
                    
                    # Calculate AOV
                    if revenues and orders:
                        aov_values = [
                            r.get("avg_order_value", 0) 
                            for r in daily_revenue 
                            if r.get("avg_order_value")
                        ]
                        if aov_values:
                            metrics["kpis"]["average_order_value"] = round(statistics.mean(aov_values), 2)
                        else:
                            metrics["kpis"]["average_order_value"] = round(
                                total_revenue / sum(orders), 2
                            )
        
        # Extract customer metrics
        if "Customer" in self.query_results:
            segmentation = self.query_results["Customer"].get("customer_segmentation", {}).get("data", [])
            if segmentation:
                # Calculate customer distribution
                total_customers = sum(s.get("customer_count", 0) for s in segmentation)
                high_value = next((s for s in segmentation if "High" in s.get("segment", "")), None)
                
                if total_customers > 0:
                    metrics["ratios"]["high_value_ratio"] = round(
                        (high_value.get("customer_count", 0) / total_customers * 100) 
                        if high_value else 0, 2
                    )
                
                # Calculate average LTV
                ltv_values = [s.get("avg_ltv", 0) for s in segmentation if s.get("avg_ltv")]
                if ltv_values:
                    metrics["kpis"]["average_ltv"] = round(statistics.mean(ltv_values), 2)
            
            # Calculate retention metrics
            retention = self.query_results["Customer"].get("customer_retention_cohort", {}).get("data", [])
            if retention:
                avg_retention_months = [
                    r.get("avg_active_months", 0) 
                    for r in retention 
                    if r.get("avg_active_months")
                ]
                if avg_retention_months:
                    metrics["kpis"]["avg_customer_lifespan_months"] = round(statistics.mean(avg_retention_months), 1)
        
        # Extract product metrics
        if "Product" in self.query_results:
            product_ranking = self.query_results["Product"].get("product_sales_ranking", {}).get("data", [])
            if product_ranking:
                total_units = sum(p.get("total_units_sold", 0) for p in product_ranking)
                total_product_revenue = sum(p.get("total_revenue", 0) for p in product_ranking)
                
                metrics["kpis"]["top_product_units_sold"] = total_units
                metrics["kpis"]["top_product_revenue"] = round(total_product_revenue, 2)
        
        # Extract operational metrics
        if "Operations" in self.query_results:
            fulfillment = self.query_results["Operations"].get("order_fulfillment_time", {}).get("data", [])
            if fulfillment:
                fulfillment_hours = [
                    f.get("avg_fulfillment_hours", 0) 
                    for f in fulfillment 
                    if f.get("avg_fulfillment_hours")
                ]
                if fulfillment_hours:
                    metrics["kpis"]["avg_fulfillment_hours"] = round(statistics.mean(fulfillment_hours), 1)
                
                fulfillment_rates = [
                    f.get("fulfillment_rate", 0) 
                    for f in fulfillment 
                    if f.get("fulfillment_rate")
                ]
                if fulfillment_rates:
                    metrics["ratios"]["avg_fulfillment_rate"] = round(statistics.mean(fulfillment_rates), 2)
            
            # Return rate
            returns = self.query_results["Operations"].get("return_rate_analysis", {}).get("data", [])
            if returns:
                return_rates = [r.get("return_rate", 0) for r in returns if r.get("return_rate")]
                if return_rates:
                    metrics["ratios"]["return_rate"] = round(statistics.mean(return_rates), 2)
        
        # Calculate trends
        if "Revenue" in self.query_results:
            trend_data = self.query_results["Revenue"].get("daily_revenue_trend", {}).get("data", [])
            if len(trend_data) >= 14:
                recent = [d.get("revenue", 0) for d in trend_data[-7:]]
                older = [d.get("revenue", 0) for d in trend_data[-14:-7]]
                
                if sum(older) > 0:
                    trend_direction = sum(recent) - sum(older)
                    metrics["trends"]["revenue_trend"] = "increasing" if trend_direction > 0 else "decreasing"
        
        self.business_metrics = metrics
        
        print(f"{Colors.GREEN}✓ Calculated {len(metrics['kpis'])} KPIs{Colors.END}")
        print(f"{Colors.GREEN}✓ Calculated {len(metrics['ratios'])} ratios{Colors.END}")
        print(f"{Colors.GREEN}✓ Identified {len(metrics['trends'])} trends{Colors.END}\n")
        
        return metrics
    
    def detect_anomalies(self) -> Dict[str, Any]:
        """Detect anomalies in business metrics."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Detecting Anomalies{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        anomalies = {
            "anomalies": [],
            "patterns": {}
        }
        
        # Analyze revenue for spikes/drops
        if "Revenue" in self.query_results:
            daily_data = self.query_results["Revenue"].get("daily_revenue_trend", {}).get("data", [])
            
            if daily_data and len(daily_data) >= 7:
                revenues = [d.get("revenue", 0) for d in daily_data]
                avg_revenue = statistics.mean(revenues)
                std_revenue = statistics.stdev(revenues) if len(revenues) > 1 else 0
                
                for i, day in enumerate(daily_data):
                    revenue = day.get("revenue", 0)
                    deviation = (revenue - avg_revenue) / std_revenue if std_revenue > 0 else 0
                    
                    if abs(deviation) > 2:  # More than 2 standard deviations
                        anomalies["anomalies"].append({
                            "metric": "daily_revenue",
                            "date": day.get("date"),
                            "actual_value": revenue,
                            "expected_value": round(avg_revenue, 2),
                            "deviation": round(deviation, 2),
                            "type": "spike" if deviation > 0 else "drop",
                            "z_score": round(deviation, 2)
                        })
                
                # Detect weekly patterns
                if len(daily_data) >= 14:
                    weekdays = defaultdict(list)
                    for day in daily_data:
                        date = day.get("date")
                        if date:
                            weekday = date.strftime("%A") if isinstance(date, str) else "Unknown"
                            weekdays[weekday].append(day.get("revenue", 0))
                    
                    weekday_avgs = {w: statistics.mean(v) for w, v in weekdays.items()}
                    if weekday_avgs:
                        peak_day = max(weekday_avgs, key=weekday_avgs.get)
                        low_day = min(weekday_avgs, key=weekday_avgs.get)
                        
                        anomalies["patterns"]["weekly_seasonality"] = {
                            "peak_day": peak_day,
                            "peak_revenue": round(weekday_avgs[peak_day], 2),
                            "low_day": low_day,
                            "low_revenue": round(weekday_avgs[low_day], 2),
                            "spread": round(
                                (weekday_avgs[peak_day] - weekday_avgs[low_day]) / weekday_avgs[low_day] * 100, 1
                            ) if weekday_avgs[low_day] > 0 else 0
                        }
        
        # Analyze conversion rate changes
        if "Marketing" in self.query_results:
            funnel = self.query_results["Marketing"].get("conversion_funnel", {}).get("data", [])
            if funnel and len(funnel) >= 3:
                # Check for significant drop-offs
                for i in range(1, len(funnel)):
                    prev_count = funnel[i-1].get("count", 0)
                    curr_count = funnel[i].get("count", 0)
                    
                    if prev_count > 0:
                        drop_rate = (prev_count - curr_count) / prev_count * 100
                        
                        if drop_rate > 50:  # More than 50% drop-off
                            stage = funnel[i].get("stage", "Unknown")
                            anomalies["anomalies"].append({
                                "metric": "funnel_drop_off",
                                "stage": stage,
                                "drop_rate": round(drop_rate, 2),
                                "type": "drop_off",
                                "severity": "high" if drop_rate > 70 else "medium"
                            })
        
        self.metadata["anomalies"] = anomalies
        
        print(f"{Colors.GREEN}✓ Found {len(anomalies['anomalies'])} anomalies{Colors.END}")
        print(f"{Colors.GREEN}✓ Identified {len(anomalies['patterns'])} patterns{Colors.END}\n")
        
        return anomalies
    
    def generate_insights(self) -> List[Dict[str, Any]]:
        """Generate actionable business insights."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Generating Business Insights{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        insights = []
        
        # Revenue insights
        total_revenue = self.business_metrics.get("kpis", {}).get("total_revenue", 0)
        revenue_growth = self.business_metrics.get("kpis", {}).get("revenue_growth_wo_w", 0)
        aov = self.business_metrics.get("kpis", {}).get("average_order_value", 0)
        
        if total_revenue > 0:
            if revenue_growth > 10:
                insights.append({
                    "type": "opportunity",
                    "title": "Strong Revenue Growth Momentum",
                    "description": f"Revenue grew {revenue_growth:.1f}% week-over-week, reaching ${total_revenue:,.0f} this period.",
                    "impact": "high",
                    "recommendation": "Capitalize on this momentum by increasing marketing spend on top-performing channels and considering promotional campaigns."
                })
            elif revenue_growth < -10:
                insights.append({
                    "type": "risk",
                    "title": "Revenue Declining Significantly",
                    "description": f"Revenue dropped {abs(revenue_growth):.1f}% week-over-week, indicating potential issues.",
                    "impact": "high",
                    "recommendation": "Investigate root causes immediately - check for technical issues, competitor activity, or marketing campaign performance."
                })
        
        # Customer insights
        high_value_ratio = self.business_metrics.get("ratios", {}).get("high_value_ratio", 0)
        avg_ltv = self.business_metrics.get("kpis", {}).get("average_ltv", 0)
        
        if high_value_ratio > 0 and avg_ltv > 0:
            if high_value_ratio > 20:
                insights.append({
                    "type": "opportunity",
                    "title": "Strong High-Value Customer Base",
                    "description": f"High-value customers represent {high_value_ratio:.1f}% of your base with an average LTV of ${avg_ltv:.0f}.",
                    "impact": "high",
                    "recommendation": "Focus on retention programs and personalized offers for this segment to maximize LTV and word-of-mouth referrals."
                })
        
        # Operational insights
        fulfillment_hours = self.business_metrics.get("kpis", {}).get("avg_fulfillment_hours", 0)
        return_rate = self.business_metrics.get("ratios", {}).get("return_rate", 0)
        
        if fulfillment_hours > 0:
            if fulfillment_hours > 48:
                insights.append({
                    "type": "risk",
                    "title": "Slow Order Fulfillment",
                    "description": f"Average fulfillment time is {fulfillment_hours:.1f} hours, which may impact customer satisfaction.",
                    "impact": "medium",
                    "recommendation": "Review fulfillment operations - consider faster shipping options or warehouse optimization."
                })
            elif fulfillment_hours < 24:
                insights.append({
                    "type": "opportunity",
                    "title": "Excellent Fulfillment Speed",
                    "description": f"Average fulfillment time is {fulfillment_hours:.1f} hours, well above industry average.",
                    "impact": "medium",
                    "recommendation": "Highlight fast shipping in marketing materials as a competitive advantage."
                })
        
        if return_rate > 0:
            if return_rate > 15:
                insights.append({
                    "type": "risk",
                    "title": "High Return Rate",
                    "description": f"Return rate is {return_rate:.1f}%, significantly above industry benchmark of 8-10%.",
                    "impact": "high",
                    "recommendation": "Analyze return reasons, improve product descriptions and photos, consider size guides for apparel."
                })
            elif return_rate < 5:
                insights.append({
                    "type": "opportunity",
                    "title": "Low Return Rate Advantage",
                    "description": f"Return rate of {return_rate:.1f}% is well below industry average, indicating high customer satisfaction.",
                    "impact": "medium",
                    "recommendation": "Use low return rate as a selling point in marketing campaigns."
                })
        
        # Anomaly-based insights
        anomalies = self.metadata.get("anomalies", {})
        for anomaly in anomalies.get("anomalies", []):
            if anomaly.get("type") == "spike" and anomaly.get("deviation", 0) > 2:
                insights.append({
                    "type": "insight",
                    "title": f"Revenue Spike Detected on {anomaly.get('date')}",
                    "description": f"Revenue was {anomaly.get('z_score'):.1f} standard deviations above average.",
                    "impact": "medium",
                    "recommendation": "Identify what drove this spike - consider marketing campaigns, product launches, or external events."
                })
        
        self.insights = insights
        
        print(f"{Colors.GREEN}✓ Generated {len(insights)} insights{Colors.END}")
        print(f"   - Opportunities: {len([i for i in insights if i['type'] == 'opportunity'])}")
        print(f"   - Risks: {len([i for i in insights if i['type'] == 'risk'])}")
        print(f"   - Other insights: {len([i for i in insights if i['type'] not in ['opportunity', 'risk']])}\n")
        
        return insights
    
    def generate_business_report(self, output_format: str = "markdown") -> str:
        """Generate comprehensive business intelligence report."""
        print(f"\n{Colors.CYAN}{'='*60}{Colors.END}")
        print(f"{Colors.CYAN}Generating Business Intelligence Report{Colors.END}")
        print(f"{Colors.CYAN}{'='*60}{Colors.END}\n")
        
        # Build report structure
        report = {
            "title": "Business Intelligence Report",
            "generated_at": datetime.now().isoformat(),
            "analysis_period": f"Last {self.date_range_days} days",
            "database": self.config.database,
            "executive_summary": self._build_executive_summary(),
            "key_metrics": self.business_metrics.get("kpis", {}),
            "business_ratios": self.business_metrics.get("ratios", {}),
            "trends": self.business_metrics.get("trends", {}),
            "insights": self.insights,
            "detailed_analytics": self._build_detailed_analytics(),
            "anomalies": self.metadata.get("anomalies", {}),
            "recommendations": self._build_recommendations(),
            "metadata": {
                "total_queries": self.metadata.get("execution_summary", {}).get("total_queries", 0),
                "successful_queries": self.metadata.get("execution_summary", {}).get("successful", 0),
                "tables_analyzed": len(self.metadata.get("tables", [])),
                "relationships_discovered": len(self.metadata.get("relationships", []))
            }
        }
        
        # Format output
        if output_format == "json":
            report_str = json.dumps(report, indent=2, default=str)
        else:
            report_str = self._format_markdown_report(report)
        
        # Save report
        output_file = self.output_dir / f"business_intelligence_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(output_file, 'w') as f:
            f.write(report_str)
        
        print(f"{Colors.GREEN}✓ Report saved to: {output_file}{Colors.END}\n")
        
        return report_str
    
    def _build_executive_summary(self) -> Dict[str, Any]:
        """Build executive summary section."""
        summary = {
            "overall_health": "healthy",
            "total_revenue": 0,
            "total_orders": 0,
            "average_order_value": 0,
            "customer_acquisition": 0,
            "operational_efficiency": 0,
            "critical_alerts": [],
            "key_wins": []
        }
        
        kpis = self.business_metrics.get("kpis", {})
        ratios = self.business_metrics.get("ratios", {})
        
        summary["total_revenue"] = kpis.get("total_revenue", 0)
        summary["total_orders"] = kpis.get("total_orders", 0)
        summary["average_order_value"] = kpis.get("average_order_value", 0)
        
        # Determine overall health
        health_score = 0
        total_factors = 0
        
        if kpis.get("revenue_growth_wo_w", 0) > 0:
            health_score += 1
        total_factors += 1
        
        if ratios.get("return_rate", 100) < 10:
            health_score += 1
        total_factors += 1
        
        if kpis.get("avg_fulfillment_hours", 999) < 48:
            health_score += 1
        total_factors += 1
        
        if health_score >= 2:
            summary["overall_health"] = "healthy"
        elif health_score >= 1:
            summary["overall_health"] = "stable"
        else:
            summary["overall_health"] = "needs_attention"
        
        # Critical alerts from insights
        for insight in self.insights:
            if insight.get("impact") == "high" and insight.get("type") == "risk":
                summary["critical_alerts"].append(insight["title"])
            elif insight.get("impact") == "high" and insight.get("type") == "opportunity":
                summary["key_wins"].append(insight["title"])
        
        return summary
    
    def _build_detailed_analytics(self) -> Dict[str, Any]:
        """Build detailed analytics section."""
        analytics = {}
        
        # Revenue breakdown
        if "Revenue" in self.query_results:
            revenue_data = self.query_results["Revenue"]
            analytics["revenue"] = {
                "daily_trend": len(revenue_data.get("daily_revenue_trend", {}).get("data", [])),
                "categories_analyzed": len(revenue_data.get("revenue_by_category", {}).get("data", [])),
                "payment_methods": len(revenue_data.get("revenue_by_payment_method", {}).get("data", []))
            }
        
        # Customer analysis
        if "Customer" in self.query_results:
            customer_data = self.query_results["Customer"]
            analytics["customers"] = {
                "segments_analyzed": len(customer_data.get("customer_segmentation", {}).get("data", [])),
                "cohorts_analyzed": len(customer_data.get("customer_retention_cohort", {}).get("data", [])),
                "activity_levels": len(customer_data.get("customer_activity_levels", {}).get("data", []))
            }
        
        # Product analysis
        if "Product" in self.query_results:
            product_data = self.query_results["Product"]
            analytics["products"] = {
                "products_ranked": len(product_data.get("product_sales_ranking", {}).get("data", [])),
                "categories_analyzed": len(product_data.get("category_performance", {}).get("data", []))
            }
        
        return analytics
    
    def _build_recommendations(self) -> List[Dict[str, Any]]:
        """Build actionable recommendations."""
        recommendations = []
        
        for insight in self.insights:
            if insight.get("impact") in ["high", "medium"]:
                recommendations.append({
                    "priority": "high" if insight.get("impact") == "high" else "medium",
                    "title": insight.get("title", ""),
                    "description": insight.get("description", ""),
                    "action": insight.get("recommendation", "")
                })
        
        # Add data-driven recommendations
        kpis = self.business_metrics.get("kpis", {})
        ratios = self.business_metrics.get("ratios", {})
        
        if kpis.get("average_order_value", 0) < 100:
            recommendations.append({
                "priority": "medium",
                "title": "Increase Average Order Value",
                "description": "Current AOV suggests room for upselling and cross-selling.",
                "action": "Implement product bundling, free shipping thresholds, and personalized product recommendations."
            })
        
        return recommendations
    
    def _format_markdown_report(self, report: Dict[str, Any]) -> str:
        """Format report as Markdown."""
        lines = []
        
        # Title
        lines.append(f"# 📊 Business Intelligence Report")
        lines.append(f"")
        lines.append(f"**Generated:** {report['generated_at']}")
        lines.append(f"**Analysis Period:** {report['analysis_period']}")
        lines.append(f"**Database:** {report['database']}")
        lines.append(f"")
        
        # Executive Summary
        lines.append(f"## 🎯 Executive Summary")
        lines.append(f"")
        
        summary = report['executive_summary']
        health_emoji = "🟢" if summary['overall_health'] == "healthy" else ("🟡" if summary['overall_health'] == "stable" else "🔴")
        lines.append(f"**Overall Health:** {health_emoji} {summary['overall_health'].upper()}")
        lines.append(f"")
        
        # Key Metrics
        lines.append(f"### Key Metrics")
        lines.append(f"")
        lines.append(f"| Metric | Value |")
        lines.append(f"|--------|-------|")
        lines.append(f"| Total Revenue | ${summary['total_revenue']:,.2f} |")
        lines.append(f"| Total Orders | {summary['total_orders']:,} |")
        lines.append(f"| Average Order Value | ${summary['average_order_value']:.2f} |")
        lines.append(f"")
        
        # Critical Alerts
        if summary['critical_alerts']:
            lines.append(f"### 🚨 Critical Alerts")
            lines.append(f"")
            for alert in summary['critical_alerts']:
                lines.append(f"- {alert}")
            lines.append(f"")
        
        # Key Wins
        if summary['key_wins']:
            lines.append(f"### 🏆 Key Wins")
            lines.append(f"")
            for win in summary['key_wins']:
                lines.append(f"- {win}")
            lines.append(f"")
        
        # Key Metrics Detail
        lines.append(f"## 📈 Key Performance Indicators")
        lines.append(f"")
        lines.append(f"| KPI | Value | Status |")
        lines.append(f"|-----|-------|--------|")
        
        for kpi, value in report['key_metrics'].items():
            kpi_name = kpi.replace("_", " ").title()
            status = "✅" if value else "⚠️"
            if "rate" in kpi.lower() or "ratio" in kpi.lower():
                formatted_value = f"{value}%"
            elif "revenue" in kpi.lower() or "value" in kpi.lower() or "ltv" in kpi.lower():
                formatted_value = f"${value:,.2f}"
            elif "orders" in kpi.lower():
                formatted_value = f"{value:,.0f}"
            else:
                formatted_value = str(value)
            lines.append(f"| {kpi_name} | {formatted_value} | {status} |")
        
        lines.append(f"")
        
        # Business Ratios
        if report['business_ratios']:
            lines.append(f"## 📊 Business Ratios")
            lines.append(f"")
            lines.append(f"| Ratio | Value | Benchmark |")
            lines.append("|-------|-------|-----------|")
            
            benchmarks = {
                "return_rate": ("Return Rate", "8-10%"),
                "fulfillment_rate": ("Fulfillment Rate", ">95%"),
                "high_value_ratio": ("High-Value Customer %", "15-20%"),
                "conversion_rate": ("Conversion Rate", "2-5%")
            }
            
            for ratio, value in report['business_ratios'].items():
                ratio_display = ratio.replace("_", " ").title()
                benchmark = benchmarks.get(ratio, ("", ""))[1]
                formatted_value = f"{value}%" if value < 100 else f"{value:,.2f}"
                lines.append(f"| {ratio_display} | {formatted_value} | {benchmark} |")
            
            lines.append(f"")
        
        # Insights
        if report['insights']:
            lines.append(f"## 💡 Insights & Analysis")
            lines.append(f"")
            
            for i, insight in enumerate(report['insights'], 1):
                emoji = "🚀" if insight['type'] == "opportunity" else ("⚠️" if insight['type'] == "risk" else "💡")
                lines.append(f"### {emoji} {i}. {insight['title']}")
                lines.append(f"")
                lines.append(f"**Impact:** {insight['impact'].upper()}")
                lines.append(f"")
                lines.append(f"{insight['description']}")
                lines.append(f"")
                lines.append(f"**Recommendation:** {insight['recommendation']}")
                lines.append(f"")
        
        # Recommendations
        if report['recommendations']:
            lines.append(f"## 🎯 Action Items")
            lines.append(f"")
            
            for rec in report['recommendations']:
                priority_emoji = "🔴" if rec['priority'] == "high" else "🟡"
                lines.append(f"{priority_emoji} **{rec['title']}**")
                lines.append(f"")
                lines.append(f"{rec['action']}")
                lines.append(f"")
        
        # Anomalies
        anomalies = report.get('anomalies', {})
        if anomalies.get('anomalies'):
            lines.append(f"## 🔍 Detected Anomalies")
            lines.append(f"")
            
            for anomaly in anomalies['anomalies']:
                type_emoji = "📈" if anomaly['type'] == "spike" else "📉"
                lines.append(f"{type_emoji} **{anomaly['metric'].title()}** - {anomaly['date']}")
                lines.append(f"- Deviation: {anomaly['z_score']:.1f}σ from average")
                lines.append(f"- Actual: {anomaly['actual_value']:,.2f} vs Expected: {anomaly['expected_value']:,.2f}")
                lines.append(f"")
        
        # Metadata
        lines.append(f"## 📋 Report Metadata")
        lines.append(f"")
        lines.append(f"- **Queries Executed:** {report['metadata']['total_queries']}")
        lines.append(f"- **Successful Queries:** {report['metadata']['successful_queries']}")
        lines.append(f"- **Tables Analyzed:** {report['metadata']['tables_analyzed']}")
        lines.append(f"- **Relationships Discovered:** {report['metadata']['relationships_discovered']}")
        lines.append(f"")
        
        # Footer
        lines.append(f"---")
        lines.append(f"*Report generated by PostgreSQL Business Intelligence Agent*")
        lines.append(f"*Next report recommended in 7 days*")
        
        return "\n".join(lines)
    
    # =========================================================================
    # MAIN WORKFLOW
    # =========================================================================
    
    def run_full_analysis(self):
        """Execute the complete business intelligence analysis workflow."""
        print(f"\n{Colors.BOLD}{Colors.CYAN}")
        print("╔════════════════════════════════════════════════════════════════╗")
        print("║     PostgreSQL Business Intelligence Agent v1.0                ║")
        print("╚════════════════════════════════════════════════════════════════╝")
        print(f"{Colors.END}\n")
        
        # Step 1: Connect to database
        if not self.connect():
            return False
        
        try:
            # Step 2: Discover metadata
            self.discover_database_metadata()
            
            # Step 3: Identify business tables
            self.identify_business_tables()
            
            # Step 4: Sample data
            self.sample_table_data()
            
            # Step 5: Detect patterns
            self.detect_data_patterns()
            
            # Step 6: Infer business context
            self.infer_business_context()
            
            # Step 7: Generate queries
            query_lists = {
                "Revenue": self.generate_revenue_queries(),
                "Customer": self.generate_customer_analytics_queries(),
                "Product": self.generate_product_analytics_queries(),
                "Operations": self.generate_operational_analytics_queries(),
                "Marketing": self.generate_marketing_analytics_queries()
            }
            
            # Step 8: Execute queries
            self.execute_all_queries(query_lists)
            
            # Step 9: Calculate metrics
            self.calculate_business_metrics()
            
            # Step 10: Detect anomalies
            self.detect_anomalies()
            
            # Step 11: Generate insights
            self.generate_insights()
            
            # Step 12: Generate report
            report = self.generate_business_report()
            
            # Print report summary
            print(f"\n{Colors.BOLD}{Colors.GREEN}")
            print("╔════════════════════════════════════════════════════════════════╗")
            print("║                    ANALYSIS COMPLETE                            ║")
            print("╚════════════════════════════════════════════════════════════════╝")
            print(f"{Colors.END}\n")
            
            # Print quick stats
            summary = self.metadata.get("execution_summary", {})
            print(f"📊 Queries: {summary.get('total_queries', 0)} executed "
                  f"({summary.get('successful', 0)} successful)")
            print(f"💰 KPIs Identified: {len(self.business_metrics.get('kpis', {}))}")
            print(f"⚠️  Insights Generated: {len(self.insights)}")
            print(f"🚨 Anomalies Detected: {len(self.metadata.get('anomalies', {}).get('anomalies', []))}")
            print()
            
            # Print key metrics
            print(f"{Colors.CYAN}Key Metrics:{Colors.END}")
            kpis = self.business_metrics.get("kpis", {})
            for kpi, value in list(kpis.items())[:5]:
                print(f"  - {kpi.replace('_', ' ').title()}: {value}")
            print()
            
            # Print top insights
            if self.insights:
                print(f"{Colors.CYAN}Top Insights:{Colors.END}")
                for insight in self.insights[:3]:
                    emoji = "🚀" if insight['type'] == "opportunity" else "⚠️"
                    print(f"  {emoji} {insight['title']}")
                print()
            
            return True
            
        finally:
            self.disconnect()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="PostgreSQL Business Intelligence Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full analysis
  python3 business_intelligence_agent.py --full-analysis
  
  # Generate specific queries
  python3 business_intelligence_agent.py --skill generate_revenue_queries
  python3 business_intelligence_agent.py --skill generate_customer_analytics_queries
  
  # Analyze specific period
  python3 business_intelligence_agent.py --full-analysis --date-range 90
  
  # Generate JSON report
  python3 business_intelligence_agent.py --skill generate_business_report --format json
        """
    )
    
    parser.add_argument("--skill", "-s", help="Run a specific skill")
    parser.add_argument("--full-analysis", "-f", action="store_true", 
                       help="Run the complete analysis workflow")
    parser.add_argument("--config", "-c", default="../../assets/db_config.env",
                       help="Path to database configuration file")
    parser.add_argument("--output", "-o", default="output",
                       help="Output directory for reports")
    parser.add_argument("--format", default="markdown",
                       choices=["markdown", "json"], help="Report format")
    parser.add_argument("--sample-size", type=int, default=1000,
                       help="Number of rows to sample per table")
    parser.add_argument("--date-range", type=int, default=30,
                       help="Analysis date range in days")
    parser.add_argument("--tables", help="Comma-separated list of tables to analyze")
    
    args = parser.parse_args()
    
    # Load configuration - use environment variables first
    import os
    config = DatabaseConfig()
    
    if os.environ.get('PGHOST'):
        config.host = os.environ.get('PGHOST', '127.0.0.1')
        config.port = int(os.environ.get('PGPORT', 5432))
        config.user = os.environ.get('PGUSER', 'digoal')
        config.password = os.environ.get('PGPASSWORD', '')
        config.database = os.environ.get('PGDATABASE', 'postgres')
    else:
        # Try config file
        config_path = Path(__file__).parent / args.config
        if config_path.exists():
            with open(config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        if key == "PGHOST":
                            config.host = value
                        elif key == "PGPORT":
                            config.port = int(value)
                        elif key == "PGUSER":
                            config.user = value
                        elif key == "PGPASSWORD":
                            config.password = value
                        elif key == "PGDATABASE":
                            config.database = value
                    
                    if key == "PGHOST":
                        config.host = value
                    elif key == "PGPORT":
                        config.port = int(value)
                    elif key == "PGUSER":
                        config.user = value
                    elif key == "PGPASSWORD":
                        config.password = value
                    elif key == "PGDATABASE":
                        config.database = value
    
    # Initialize agent
    agent = PostgreSQLBIAgent(
        config=config,
        sample_size=args.sample_size,
        date_range_days=args.date_range,
        output_dir=args.output
    )
    
    # Execute
    if args.full_analysis:
        agent.run_full_analysis()
    elif args.skill:
        if not agent.connect():
            sys.exit(1)
        
        try:
            if args.skill == "discover_database_metadata":
                metadata = agent.discover_database_metadata()
                print(json.dumps(metadata, indent=2))
            elif args.skill == "identify_business_tables":
                tables = agent.identify_business_tables()
                print(json.dumps(tables, indent=2))
            elif args.skill == "sample_table_data":
                samples = agent.sample_table_data()
                print(json.dumps(samples, indent=2))
            elif args.skill == "detect_data_patterns":
                patterns = agent.detect_data_patterns()
                print(json.dumps(patterns, indent=2))
            elif args.skill == "infer_business_context":
                context = agent.infer_business_context()
                print(json.dumps(context, indent=2))
            elif args.skill == "generate_revenue_queries":
                queries = agent.generate_revenue_queries()
                print(json.dumps([{"name": q.name, "sql": q.sql} for q in queries], indent=2))
            elif args.skill == "generate_customer_analytics_queries":
                queries = agent.generate_customer_analytics_queries()
                print(json.dumps([{"name": q.name, "sql": q.sql} for q in queries], indent=2))
            elif args.skill == "generate_product_analytics_queries":
                queries = agent.generate_product_analytics_queries()
                print(json.dumps([{"name": q.name, "sql": q.sql} for q in queries], indent=2))
            elif args.skill == "generate_operational_analytics_queries":
                queries = agent.generate_operational_analytics_queries()
                print(json.dumps([{"name": q.name, "sql": q.sql} for q in queries], indent=2))
            elif args.skill == "generate_marketing_analytics_queries":
                queries = agent.generate_marketing_analytics_queries()
                print(json.dumps([{"name": q.name, "sql": q.sql} for q in queries], indent=2))
            elif args.skill == "execute_bi_queries":
                query_lists = {
                    "Revenue": agent.generate_revenue_queries(),
                    "Customer": agent.generate_customer_analytics_queries(),
                    "Product": agent.generate_product_analytics_queries(),
                    "Operations": agent.generate_operational_analytics_queries(),
                    "Marketing": agent.generate_marketing_analytics_queries()
                }
                results = agent.execute_all_queries(query_lists)
                print(json.dumps(results, indent=2, default=str))
            elif args.skill == "calculate_business_metrics":
                agent.discover_database_metadata()
                agent.identify_business_tables()
                metrics = agent.calculate_business_metrics()
                print(json.dumps(metrics, indent=2))
            elif args.skill == "detect_anomalies":
                anomalies = agent.detect_anomalies()
                print(json.dumps(anomalies, indent=2))
            elif args.skill == "generate_insights":
                agent.discover_database_metadata()
                agent.identify_business_tables()
                insights = agent.generate_insights()
                print(json.dumps(insights, indent=2))
            elif args.skill == "generate_business_report":
                agent.discover_database_metadata()
                agent.identify_business_tables()
                agent.sample_table_data()
                agent.detect_data_patterns()
                agent.infer_business_context()
                query_lists = {
                    "Revenue": agent.generate_revenue_queries(),
                    "Customer": agent.generate_customer_analytics_queries(),
                    "Product": agent.generate_product_analytics_queries(),
                    "Operations": agent.generate_operational_analytics_queries(),
                    "Marketing": agent.generate_marketing_analytics_queries()
                }
                agent.execute_all_queries(query_lists)
                agent.calculate_business_metrics()
                agent.detect_anomalies()
                agent.generate_insights()
                report = agent.generate_business_report(args.format)
                print(report)
            else:
                print(f"{Colors.RED}Unknown skill: {args.skill}{Colors.END}")
                sys.exit(1)
        finally:
            agent.disconnect()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
