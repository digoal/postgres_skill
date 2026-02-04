---
name: postgresql-bi-agent
description: Business intelligence analysis agent for PostgreSQL that discovers database metadata, generates SQL queries for revenue/customer/product metrics, and produces comprehensive analytical reports
license: MIT
compatibility: opencode
---
  
# PostgreSQL Business Intelligence Agent

This skill guides the agent in conducting comprehensive business intelligence analysis on a PostgreSQL database. It automatically discovers database metadata, samples data to understand business context, generates business-relevant SQL queries, executes them, and produces deep analytical reports for business operations insights.

## Purpose

The primary goal of this skill is to transform raw database data into actionable business intelligence. It serves as an automated data analyst that:

- **Discovers** database structure and metadata automatically
- **Understands** business context through data sampling
- **Generates** relevant SQL queries for business metrics
- **Executes** queries efficiently and safely
- **Analyzes** results to provide strategic insights
- **Reports** findings with actionable recommendations

## Business Intelligence Framework

The agent uses a multi-dimensional framework covering:

### 1. Business Scale Metrics
- Transaction volumes and values
- Customer base and distribution
- Product/service inventory and categorization

### 2. Growth Trends
- Year-over-year and month-over-month comparisons
- Daily/Monthly active user trends
- Revenue and order trends

### 3. User Behavior Analysis
- User engagement and activity levels
- Retention rates and cohort analysis
- Conversion funnel metrics

### 4. Financial Insights
- Revenue trends and patterns
- Average order value (AOV)
- Payment success rates and refund metrics

### 5. Inventory & Product Analysis
- Inventory turnover rates
- Product performance rankings
- Stock-out and overstock indicators

### 6. Operational Efficiency
- Order processing times
- Customer satisfaction indicators
- System performance metrics

## Workflow

When activated, this skill executes a comprehensive analysis pipeline:

1. **Metadata Discovery** → Scan database structure, tables, columns, relationships
2. **Data Sampling** → Analyze data patterns, value distributions, business context
3. **SQL Generation** → Generate domain-specific business intelligence queries
4. **Query Execution** → Run all queries with proper error handling
5. **Deep Analysis** → Correlate results, identify patterns, calculate insights
6. **Report Generation** → Produce comprehensive business report

## Available Skills

Each skill represents a callable analysis module that returns structured JSON output.

---

## 1. Metadata Discovery

### Skill: `discover_database_metadata`

-   **Description**: Scans and collects comprehensive database metadata including schemas, tables, columns, data types, constraints, indexes, and relationships.
-   **Usage**: `python3 business_intelligence_agent.py --skill discover_database_metadata`
-   **Expected Output**:
    ```json
    {
      "skill": "discover_database_metadata",
      "status": "success",
      "data": {
        "database_name": "ecommerce_db",
        "schemas": ["public", "analytics", "audit"],
        "total_tables": 45,
        "total_columns": 380,
        "tables": [
          {
            "schema_name": "public",
            "table_name": "orders",
            "row_count": 1500000,
            "columns": [
              {"name": "order_id", "type": "bigint", "is_pk": true},
              {"name": "user_id", "type": "bigint", "is_fk": true},
              {"name": "order_amount", "type": "decimal(12,2)", "is_nullable": false},
              {"name": "created_at", "type": "timestamp", "is_nullable": false}
            ],
            "indexes": ["idx_orders_user_id", "idx_orders_created_at"]
          }
        ],
        "relationships": [
          {"from": "orders.user_id", "to": "users.user_id", "type": "FK"}
        ]
      }
    }
    ```

### Skill: `identify_business_tables`

-   **Description**: Identifies tables likely related to core business operations based on naming conventions, column patterns, and data characteristics.
-   **Usage**: `python3 business_intelligence_agent.py --skill identify_business_tables`
-   **Expected Output**:
    ```json
    {
      "skill": "identify_business_tables",
      "status": "success",
      "data": {
        "core_tables": [
          {"name": "orders", "category": "transactions", "confidence": 0.95},
          {"name": "users", "category": "customers", "confidence": 0.92},
          {"name": "products", "category": "inventory", "confidence": 0.88}
        ],
        "transaction_tables": ["orders", "payments", "refunds"],
        "customer_tables": ["users", "customers", "accounts"],
        "product_tables": ["products", "inventory", "categories"]
      }
    }
    ```

### Skill: `analyze_table_relationships`

-   **Description**: Maps foreign key relationships and infers implicit relationships to build a complete database relationship graph.
-   **Usage**: `python3 business_intelligence_agent.py --skill analyze_table_relationships`
-   **Expected Output**:
    ```json
    {
      "skill": "analyze_table_relationships",
      "status": "success",
      "data": {
        "explicit_relationships": 25,
        "inferred_relationships": 12,
        "relationship_graph": {
          "nodes": ["orders", "users", "products", "categories", "payments"],
          "edges": [
            {"from": "orders", "to": "users", "type": "M:1"},
            {"from": "orders", "to": "payments", "type": "1:1"},
            {"from": "order_items", "to": "orders", "type": "M:1"},
            {"from": "order_items", "to": "products", "type": "M:1"}
          ]
        }
      }
    }
    ```

---

## 2. Data Sampling & Pattern Analysis

### Skill: `sample_table_data`

-   **Description**: Samples data from identified business tables to understand data distribution, value ranges, and business context.
-   **Usage**: `python3 business_intelligence_agent.py --skill sample_table_data --tables orders,users,products`
-   **Expected Output**:
    ```json
    {
      "skill": "sample_table_data",
      "status": "success",
      "data": {
        "sampled_tables": {
          "orders": {
            "sample_size": 100,
            "date_range": {"min": "2024-01-01", "max": "2024-01-15"},
            "value_ranges": {"order_amount": {"min": 10.00, "max": 15000.00, "avg": 250.50}},
            "categorical_distributions": {"payment_method": {"credit_card": 45, "debit_card": 30, "digital_wallet": 25}}
          },
          "users": {
            "sample_size": 100,
            "date_joined_range": {"min": "2023-06-01", "max": "2024-01-15"},
            "user_distribution": {"new_users_last_30_days": 15000}
          }
        }
      }
    }
    ```

### Skill: `detect_data_patterns`

-   **Description**: Analyzes temporal patterns, value distributions, and data quality indicators across business tables.
-   **Usage**: `python3 business_intelligence_agent.py --skill detect_data_patterns`
-   **Expected Output**:
    ```json
    {
      "skill": "detect_data_patterns",
      "status": "success",
      "data": {
        "temporal_patterns": {
          "orders": {"peak_hours": "12:00-14:00, 19:00-21:00", "peak_days": "Friday, Saturday"},
          "users": {"registration_spike": "Weekends", "activity_peak": "Evening 19:00-23:00"}
        },
        "value_distributions": {
          "order_amount": {"skewness": 2.5, "outliers": 150, "percentile_95": 1200.00}
        },
        "data_quality": {
          "null_rates": {"orders.shipping_address": 0.15, "orders.coupon_code": 0.85},
          "duplicate_rates": {"orders.order_id": 0.0, "users.email": 0.001}
        }
      }
    }
    ```

### Skill: `infer_business_context`

-   **Description**: Uses metadata and data patterns to infer the business domain, model type (B2B/B2C/SaaS), and key business entities.
-   **Usage**: `python3 business_intelligence_agent.py --skill infer_business_context`
-   **Expected Output**:
    ```json
    {
      "skill": "infer_business_context",
      "status": "success",
      "data": {
        "business_domain": "E-commerce Retail",
        "business_model": "B2C",
        "primary_entities": ["Orders", "Products", "Customers"],
        "secondary_entities": ["Payments", "Shipping", "Returns"],
        "key_metrics": ["GMV", "AOV", "Conversion Rate", "Customer LTV"],
        "time_granularities": ["daily", "weekly", "monthly", "quarterly"]
      }
    }
    ```

---

## 3. Business Intelligence SQL Generation

### Skill: `generate_revenue_queries`

-   **Description**: Generates SQL queries for revenue analysis including revenue trends, breakdown by category/time/payment method, and financial KPIs.
-   **Usage**: `python3 business_intelligence_agent.py --skill generate_revenue_queries`
-   **Expected Output**:
    ```json
    {
      "skill": "generate_revenue_queries",
      "status": "success",
      "queries": [
        {
          "name": "daily_revenue_trend",
          "sql": "SELECT DATE(created_at) as date, SUM(order_amount) as revenue, COUNT(*) as orders FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY DATE(created_at) ORDER BY date",
          "metrics": ["daily_revenue", "order_count", "avg_order_value"]
        },
        {
          "name": "revenue_by_category",
          "sql": "SELECT c.category_name, SUM(oi.quantity * oi.unit_price) as revenue, COUNT(DISTINCT o.order_id) as orders FROM order_items oi JOIN products p ON oi.product_id = p.id JOIN categories c ON p.category_id = c.id JOIN orders o ON oi.order_id = o.id WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY c.category_name ORDER BY revenue DESC",
          "metrics": ["category_revenue", "category_orders", "category_aov"]
        },
        {
          "name": "monthly_revenue_comparison",
          "sql": "SELECT DATE_TRUNC('month', created_at) as month, SUM(order_amount) as revenue FROM orders WHERE created_at >= CURRENT_DATE - INTERVAL '12 months' GROUP BY DATE_TRUNC('month', created_at) ORDER BY month",
          "metrics": ["monthly_revenue", "mom_growth", "yoy_growth"]
        }
      ]
    }
    ```

### Skill: `generate_customer_analytics_queries`

-   **Description**: Generates SQL for customer analytics including acquisition, retention, segmentation, LTV, and behavioral analysis.
-   **Usage**: `python3 business_intelligence_agent.py --skill generate_customer_analytics_queries`
-   **Expected Output**:
    ```json
    {
      "skill": "generate_customer_analytics_queries",
      "status": "success",
      "queries": [
        {
          "name": "customer_acquisition_trend",
          "sql": "SELECT DATE_TRUNC('week', created_at) as week, COUNT(*) as new_customers FROM users WHERE created_at >= CURRENT_DATE - INTERVAL '12 weeks' GROUP BY DATE_TRUNC('week', created_at) ORDER BY week",
          "metrics": ["weekly_new_users", "user_growth_rate"]
        },
        {
          "name": "customer_retention_cohort",
          "sql": "WITH cohorts AS (SELECT user_id, DATE_TRUNC('month', first_purchase) as cohort_month, COUNT(DISTINCT DATE_TRUNC('month', order_date)) as active_months FROM orders o JOIN users u ON o.user_id = u.id GROUP BY user_id, DATE_TRUNC('month', first_purchase)) SELECT cohort_month, AVG(active_months) as avg_active_months FROM cohorts GROUP BY cohort_month ORDER BY cohort_month",
          "metrics": ["cohort_retention", "customer_lifespan"]
        },
        {
          "name": "customer_segmentation",
          "sql": "SELECT CASE WHEN total_spent < 100 THEN 'Low Value' WHEN total_spent < 500 THEN 'Medium Value' ELSE 'High Value' END as segment, COUNT(*) as customer_count, AVG(order_count) as avg_orders FROM (SELECT user_id, SUM(order_amount) as total_spent, COUNT(*) as order_count FROM orders GROUP BY user_id) t GROUP BY segment",
          "metrics": ["segment_distribution", "segment_aov", "customer_value"]
        }
      ]
    }
    ```

### Skill: `generate_product_analytics_queries`

-   **Description**: Generates SQL for product analysis including sales rankings, inventory turnover, category performance, and product recommendations.
-   **Usage**: `python3 business_intelligence_agent.py --skill generate_product_analytics_queries`
-   **Expected Output**:
    ```json
    {
      "skill": "generate_product_analytics_queries",
      "status": "success",
      "queries": [
        {
          "name": "product_sales_ranking",
          "sql": "SELECT p.id, p.name, SUM(oi.quantity) as total_units_sold, SUM(oi.quantity * oi.unit_price) as total_revenue, COUNT(DISTINCT o.id) as order_count FROM products p JOIN order_items oi ON p.id = oi.product_id JOIN orders o ON oi.order_id = o.id WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY p.id, p.name ORDER BY total_revenue DESC LIMIT 20",
          "metrics": ["product_revenue", "units_sold", "product_popularity"]
        },
        {
          "name": "category_performance",
          "sql": "SELECT c.name as category, COUNT(DISTINCT p.id) as product_count, SUM(oi.quantity) as total_units, SUM(oi.quantity * oi.unit_price) as revenue, AVG(oi.quantity * oi.unit_price) as avg_order_value FROM categories c JOIN products p ON c.id = p.category_id JOIN order_items oi ON p.id = oi.product_id GROUP BY c.name ORDER BY revenue DESC",
          "metrics": ["category_revenue", "category_growth", "category_margin"]
        },
        {
          "name": "inventory_turnover",
          "sql": "SELECT p.id, p.name, p.stock_quantity, COALESCE(SUM(oi.quantity), 0) as sales_last_30d, CASE WHEN p.stock_quantity > 0 THEN ROUND((COALESCE(SUM(oi.quantity), 0)::decimal / p.stock_quantity) * 30, 2) ELSE 0 END as daily_turnover_rate FROM products p LEFT JOIN order_items oi ON p.id = oi.product_id LEFT JOIN orders o ON oi.order_id = o.id AND o.created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY p.id, p.name, p.stock_quantity ORDER BY daily_turnover_rate DESC",
          "metrics": ["turnover_rate", "stock_velocity", "reorder_point"]
        }
      ]
    }
    ```

### Skill: `generate_operational_analytics_queries`

-   **Description**: Generates SQL for operational metrics including order processing time, fulfillment rates, return rates, and customer satisfaction.
-   **Usage**: `python3 business_intelligence_agent.py --skill generate_operational_analytics_queries`
-   **Expected Output**:
    ```json
    {
      "skill": "generate_operational_analytics_queries",
      "status": "success",
      "queries": [
        {
          "name": "order_fulfillment_time",
          "sql": "SELECT DATE(o.created_at) as date, AVG(EXTRACT(EPOCH FROM (o.shipped_at - o.created_at)) / 3600) as avg_fulfillment_hours, COUNT(*) as total_orders FROM orders o WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days' AND o.shipped_at IS NOT NULL GROUP BY DATE(o.created_at) ORDER BY date",
          "metrics": ["avg_fulfillment_time", "orders_fulfilled", "fulfillment_efficiency"]
        },
        {
          "name": "return_rate_analysis",
          "sql": "SELECT DATE_TRUNC('week', o.created_at) as week, COUNT(*) as total_orders, COUNT(CASE WHEN o.status = 'returned' THEN 1 END) as returned_orders, ROUND((COUNT(CASE WHEN o.status = 'returned' THEN 1 END)::decimal / NULLIF(COUNT(*), 0)) * 100, 2) as return_rate FROM orders o WHERE o.created_at >= CURRENT_DATE - INTERVAL '12 weeks' GROUP BY DATE_TRUNC('week', o.created_at) ORDER BY week",
          "metrics": ["weekly_return_rate", "return_trend", "return_causes"]
        },
        {
          "name": "payment_success_rate",
          "sql": "SELECT DATE(created_at) as date, COUNT(*) as total_transactions, COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_payments, ROUND((COUNT(CASE WHEN status = 'completed' THEN 1 END)::decimal / NULLIF(COUNT(*), 0)) * 100, 2) as success_rate FROM payments WHERE created_at >= CURRENT_DATE - INTERVAL '30 days' GROUP BY DATE(created_at) ORDER BY date",
          "metrics": ["payment_success_rate", "payment_failures", "payment_method_performance"]
        }
      ]
    }
    ```

### Skill: `generate_marketing_analytics_queries`

-   **Description**: Generates SQL for marketing analytics including campaign performance, channel attribution, conversion funnels, and ROI analysis.
-   **Usage**: `python3 business_intelligence_agent.py --skill generate_marketing_analytics_queries`
-   **Expected Output**:
    ```json
    {
      "skill": "generate_marketing_analytics_queries",
      "status": "success",
      "queries": [
        {
          "name": "channel_attribution",
          "sql": "SELECT utm_source, COUNT(DISTINCT user_id) as users, COUNT(DISTINCT CASE WHEN has_purchase THEN user_id END) as converters, ROUND((COUNT(DISTINCT CASE WHEN has_purchase THEN user_id END)::decimal / NULLIF(COUNT(DISTINCT user_id), 0)) * 100, 2) as conversion_rate FROM (SELECT user_id, source as utm_source, MAX(CASE WHEN order_id IS NOT NULL THEN true ELSE false END) as has_purchase FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY user_id, source) t GROUP BY utm_source ORDER BY converters DESC",
          "metrics": ["channel_users", "channel_conversion", "channel_value"]
        },
        {
          "name": "conversion_funnel",
          "sql": "WITH funnel AS (SELECT 'page_view' as stage, COUNT(DISTINCT session_id) as count FROM events WHERE event_type = 'page_view' UNION ALL SELECT 'product_view' as stage, COUNT(DISTINCT session_id) as count FROM events WHERE event_type = 'product_view' UNION ALL SELECT 'add_to_cart' as stage, COUNT(DISTINCT session_id) as count FROM events WHERE event_type = 'add_to_cart' UNION ALL SELECT 'checkout' as stage, COUNT(DISTINCT session_id) as count FROM events WHERE event_type = 'checkout_start' UNION ALL SELECT 'purchase' as stage, COUNT(DISTINCT session_id) as count FROM events WHERE event_type = 'purchase') SELECT stage, count, LAG(count) OVER (ORDER BY stage) as previous_stage, ROUND((count::decimal / NULLIF(LAG(count) OVER (ORDER BY stage), 0)) * 100, 2) as conversion_pct FROM funnel",
          "metrics": ["funnel_conversion", "drop_off_rates", "stage_progression"]
        },
        {
          "name": "campaign_roi",
          "sql": "SELECT campaign_name, SUM(spend) as total_spend, COUNT(DISTINCT o.order_id) as attributed_orders, SUM(o.order_amount) as attributed_revenue, ROUND((SUM(o.order_amount) - SUM(spend))::decimal / NULLIF(SUM(spend), 0) * 100, 2) as roi_percent FROM campaigns c LEFT JOIN orders o ON c.id = o.campaign_id WHERE c.start_date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY campaign_name ORDER BY roi_percent DESC",
          "metrics": ["campaign_spend", "campaign_revenue", "campaign_roi"]
        }
      ]
    }
    ```

---

## 4. Query Execution & Analysis

### Skill: `execute_bi_queries`

-   **Description**: Executes all generated business intelligence queries and collects results with proper error handling and performance monitoring.
-   **Usage**: `python3 business_intelligence_agent.py --skill execute_bi_queries`
-   **Expected Output**:
    ```json
    {
      "skill": "execute_bi_queries",
      "status": "success",
      "execution_summary": {
        "total_queries": 25,
        "successful": 24,
        "failed": 1,
        "total_execution_time_seconds": 45.2,
        "avg_query_time_seconds": 1.88
      },
      "results": {
        "daily_revenue_trend": [{"date": "2024-01-15", "revenue": 45000.00, "orders": 180}],
        "revenue_by_category": [{"category_name": "Electronics", "revenue": 125000.00, "orders": 450}],
        "customer_segmentation": [{"segment": "High Value", "customer_count": 1500, "avg_orders": 12.5}]
      },
      "errors": [
        {"query": "complex_aggregation", "error": "timeout", "retry_count": 0}
      ]
    }
    ```

### Skill: `calculate_business_metrics`

-   **Description**: Performs calculations on query results to derive key business metrics including KPIs, ratios, trends, and benchmarks.
-   **Usage**: `python3 business_intelligence_agent.py --skill calculate_business_metrics`
-   **Expected Output**:
    ```json
    {
      "skill": "calculate_business_metrics",
      "status": "success",
      "kpis": {
        "total_revenue_30d": 1350000.00,
        "total_orders_30d": 5400,
        "average_order_value": 250.00,
        "revenue_growth_mom": 0.15,
        "revenue_growth_yoy": 0.35,
        "customer_acquisition_cost": 45.00,
        "customer_lifetime_value": 850.00,
        "gross_margin": 0.42
      },
      "ratios": {
        "repeat_purchase_rate": 0.32,
        "cart_abandonment_rate": 0.68,
        "checkout_conversion_rate": 0.045,
        "return_rate": 0.08
      },
      "trends": {
        "revenue_trend": "increasing",
        "user_growth_trend": "stable",
        "aov_trend": "increasing"
      }
    }
    ```

### Skill: `detect_anomalies`

-   **Description**: Analyzes time series data to detect anomalies, outliers, and unexpected patterns in business metrics.
-   **Usage**: `python3 business_intelligence_agent.py --skill detect_anomalies`
-   **Expected Output**:
    ```json
    {
      "skill": "detect_anomalies",
      "status": "success",
      "anomalies": [
        {
          "metric": "daily_revenue",
          "date": "2024-01-12",
          "actual_value": 85000.00,
          "expected_value": 45000.00,
          "deviation": 0.89,
          "type": "spike",
          "possible_causes": ["Holiday sale", "Marketing campaign", "Technical issue"]
        },
        {
          "metric": "conversion_rate",
          "date": "2024-01-14",
          "actual_value": 0.025,
          "expected_value": 0.045,
          "deviation": -0.44,
          "type": "drop",
          "possible_causes": ["Website issues", "Payment gateway problems", "Competitor activity"]
        }
      ],
      "patterns": {
        "weekly_seasonality": "Revenue peaks on weekends",
        "hourly_pattern": "Traffic peaks at 14:00 and 20:00",
        "monthly_pattern": "Higher sales in last week of month"
      }
    }
    ```

### Skill: `generate_insights`

-   **Description**: Uses AI-powered analysis to generate natural language insights from business data, identifying opportunities and risks.
-   **Usage**: `python3 business_intelligence_agent.py --skill generate_insights`
-   **Expected Output**:
    ```json
    {
      "skill": "generate_insights",
      "status": "success",
      "insights": [
        {
          "type": "opportunity",
          "title": "High-Value Customer Segment Shows Strong Growth",
          "description": "The high-value customer segment has grown by 25% this month with 40% higher retention rates than other segments.",
          "impact": "high",
          "recommendation": "Consider personalized marketing campaigns and exclusive offers for high-value customers to further increase engagement and loyalty."
        },
        {
          "type": "risk",
          "title": "Mobile Conversion Rate Significantly Lower Than Desktop",
          "description": "Mobile conversion rate is 2.1% compared to desktop at 4.8%, representing a significant revenue opportunity.",
          "impact": "high",
          "recommendation": "Conduct UX audit for mobile checkout flow, optimize page load times, and consider mobile-specific promotions."
        },
        {
          "type": "trend",
          "title": "Electronics Category Outperforming Overall Market",
          "description": "Electronics category grew 45% YoY compared to overall growth of 35%, driven by premium product launches.",
          "impact": "medium",
          "recommendation": "Increase inventory for top-performing electronics products and expand product range in this category."
        }
      ]
    }
    ```

---

## 5. Report Generation

### Skill: `generate_business_report`

-   **Description**: Generates a comprehensive business intelligence report combining all analysis results into a structured, actionable document.
-   **Usage**: `python3 business_intelligence_agent.py --skill generate_business_report`
-   **Expected Output**:
    ```markdown
    # Business Intelligence Report
    **Generated:** 2024-01-15
    
    ## Executive Summary
    - Total Revenue: $1.35M (↑ 15% MoM)
    - Total Orders: 5,400 (↑ 12% MoM)
    - Average Order Value: $250.00 (↑ 3% MoM)
    - Active Customers: 12,500
    
    ## Key Findings
    ### Revenue Analysis
    - Electronics category leads with $125K weekly revenue
    - Weekend sales 35% higher than weekday average
    - Top 20% of customers contribute 60% of revenue
    
    ### Customer Insights
    - High-value segment: 1,500 customers, 12.5 avg orders
    - Customer retention improved to 68%
    - Acquisition cost decreased by 8%
    
    ### Operational Metrics
    - Average fulfillment time: 24 hours
    - Return rate: 8% (within industry benchmark)
    - Payment success rate: 97.5%
    
    ## Recommendations
    1. Expand high-value customer program
    2. Optimize mobile checkout experience
    3. Increase electronics inventory
    4. Implement weekend-specific promotions
    
    ## Risks & Alerts
    - ⚠️ Conversion rate dropped on Jan 14th
    - ⚠️ Return rate increasing in clothing category
    ```
```

## Environment Setup

### Requirements

- `psql` command-line tool (PostgreSQL client, version 10+)
- Python 3.8+ with required packages:
  - `psycopg2-binary` or `pg8000` for PostgreSQL connection
  - `pandas` for data analysis
  - `numpy` for numerical calculations
  - `jinja2` for report templating
- Read-only database user with access to information_schema

### Installation

```bash
pip install psycopg2-binary pandas numpy jinja2
```

### Configuration

Configure database connection in `assets/db_config.env`:

```bash
export PGHOST="127.0.0.1"
export PGPORT="5432"
export PGUSER="readonly_user"
export PGPASSWORD="your_password"
export PGDATABASE="your_business_db"
```

### Required Permissions

```sql
-- For metadata discovery
GRANT SELECT ON information_schema.tables TO readonly_user;
GRANT SELECT ON information_schema.columns TO readonly_user;
GRANT SELECT ON information_schema.table_constraints TO readonly_user;
GRANT SELECT ON information_schema.referential_constraints TO readonly_user;
GRANT SELECT ON pg_tables TO readonly_user;
GRANT SELECT ON pg_indexes TO readonly_user;

-- For business data analysis (use read-only replica if available)
GRANT SELECT ON your_business_tables TO readonly_user;
```

---

## Usage

### Quick Start

```bash
cd postgresql-bi-agent/scripts

# Configure database connection
vi ../assets/db_config.env

# Run full business intelligence analysis
python3 business_intelligence_agent.py --full-analysis

# Run specific analysis modules
python3 business_intelligence_agent.py --skill discover_database_metadata
python3 business_intelligence_agent.py --skill generate_revenue_queries
python3 business_intelligence_agent.py --skill execute_bi_queries
python3 business_intelligence_agent.py --skill generate_business_report
```

### Command Line Options

```bash
python3 business_intelligence_agent.py [OPTIONS]

Options:
  --skill SKILL_NAME       Run a specific skill (discover_metadata, generate_queries, 
                           execute_queries, analyze_results, generate_report)
  --full-analysis          Run the complete analysis pipeline
  --tables TABLE1,TABLE2   Specify tables to analyze (comma-separated)
  --output OUTPUT_FILE     Output file path for report
  --format FORMAT          Report format: markdown, json, html
  --config CONFIG_FILE     Path to configuration file
  --sample-size N          Number of rows to sample (default: 1000)
  --date-range DAYS        Analysis date range in days (default: 30)
  --help                   Show help message
```

### Examples

```bash
# Full analysis for all tables
python3 business_intelligence_agent.py --full-analysis

# Analyze specific tables
python3 business_intelligence_agent.py --tables orders,users,products --full-analysis

# Generate report in JSON format
python3 business_intelligence_agent.py --skill generate_report --format json

# Quick revenue analysis for last 90 days
python3 business_intelligence_agent.py --skill generate_revenue_queries --date-range 90

# Focus on customer analytics
python3 business_intelligence_agent.py --skill generate_customer_analytics_queries
```

---

## Report Structure

### Executive Summary
- Overall business health score
- Key KPIs at a glance
- Critical alerts and opportunities

### Revenue Analysis
- Revenue trends (daily/weekly/monthly)
- Revenue by category/product/channel
- AOV and revenue per customer

### Customer Analytics
- Customer acquisition and retention
- Customer segmentation and LTV
- Behavioral patterns and preferences

### Product/Inventory Analysis
- Best/worst performing products
- Inventory turnover and stock levels
- Category performance

### Operational Metrics
- Order fulfillment efficiency
- Return and refund rates
- Payment success rates

### Marketing & Channels
- Channel attribution and ROI
- Conversion funnel analysis
- Campaign performance

### Recommendations
- Strategic initiatives
- Quick wins
- Risk mitigation

### Appendices
- Detailed data tables
- Methodology notes
- Data quality assessment

---

## Skill Index

### Metadata Discovery

| Skill | Description |
|-------|-------------|
| `discover_database_metadata` | Comprehensive database structure scan |
| `identify_business_tables` | Business-relevant table identification |
| `analyze_table_relationships` | Foreign key and relationship mapping |

### Data Sampling & Pattern Analysis

| Skill | Description |
|-------|-------------|
| `sample_table_data` | Data sampling for business context |
| `detect_data_patterns` | Temporal and value pattern detection |
| `infer_business_context` | Business domain inference |

### Query Generation

| Skill | Description |
|-------|-------------|
| `generate_revenue_queries` | Revenue and financial metrics SQL |
| `generate_customer_analytics_queries` | Customer lifecycle and behavior SQL |
| `generate_product_analytics_queries` | Product performance and inventory SQL |
| `generate_operational_analytics_queries` | Operational efficiency SQL |
| `generate_marketing_analytics_queries` | Marketing attribution and ROI SQL |

### Analysis & Reporting

| Skill | Description |
|-------|-------------|
| `execute_bi_queries` | Execute and collect query results |
| `calculate_business_metrics` | Derive KPIs and business metrics |
| `detect_anomalies` | Identify anomalies and outliers |
| `generate_insights` | AI-powered insight generation |
| `generate_business_report` | Comprehensive report generation |

---

## Advanced Features

### Custom Metric Definition
Users can define custom metrics by providing SQL snippets or configuration files.

### Scheduled Execution
The agent can be scheduled via cron or task schedulers for regular reporting:

```bash
# Daily report at 6 AM
0 6 * * * cd /path/to/scripts && python3 business_intelligence_agent.py --full-analysis --output /reports/daily_$(date +\%Y\%m\%d).md

# Weekly report on Mondays
0 7 * * 1 cd /path/to/scripts && python3 business_intelligence_agent.py --full-analysis --date-range 7 --output /reports/weekly_$(date +\%Y\%m\%d).md

# Monthly report on 1st
0 8 1 * * cd /path/to/scripts && python3 business_intelligence_agent.py --full-analysis --date-range 30 --output /reports/monthly_$(date +\%Y\%m).md
```

### Integration with BI Tools
- Export results to CSV/JSON for Tableau, Power BI, Looker
- API endpoint mode for real-time dashboards
- Webhook notifications for alerts

### Multi-Database Support
- Analyze multiple databases in a single report
- Cross-database relationship analysis
- Consolidated executive dashboards

---

## Best Practices

1. **Use Read-Only Replicas**: Always run queries against read replicas to avoid impacting production
2. **Schedule During Off-Peak**: Run full analyses during low-traffic periods
3. **Sample Large Tables**: Use sampling for initial discovery, full queries for final reports
4. **Monitor Query Performance**: Set appropriate timeouts for complex queries
5. **Data Quality Checks**: Validate data completeness before generating insights
6. **Incremental Processing**: Process recent data incrementally for faster daily reports

---

## Troubleshooting

### Common Issues

1. **Permission Denied**
   - Ensure user has SELECT access to information_schema and target tables
   - Check pg_hba.conf for connection permissions

2. **Query Timeout**
   - Increase timeout in configuration
   - Add indexes for frequently queried columns
   - Use sampling for large tables

3. **Memory Errors**
   - Reduce sample size
   - Process data in chunks
   - Use database-side aggregations

### Performance Optimization

- Create indexes on commonly filtered/sorted columns
- Use materialized views for complex aggregations
- Partition large tables by date
- Enable query result caching

Base directory for this skill: file:///Users/digoal/.config/opencode/skills/postgresql-bi-agent
Relative paths in this skill (e.g., scripts/, assets/) are relative to this base directory.
