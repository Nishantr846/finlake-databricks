# FinLake — Metadata-Driven Banking Lakehouse

An end-to-end banking data engineering platform built using Databricks Free Edition.

## Objective

FinLake simulates a modern banking data platform that ingests data from multiple operational domains, processes it through a Medallion Architecture, applies automated data-quality controls, maintains historical customer changes, and produces analytical data products for banking operations.

## Technology Stack

- Databricks Free Edition
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Auto Loader
- Databricks Jobs
- Databricks SQL
- Python
- Git/GitHub

## Architecture

Source Systems
→ Incremental Ingestion
→ Bronze
→ Data Quality
→ Silver
→ Gold
→ Databricks SQL
→ Dashboards

## Data Domains

- Customers
- Accounts
- Transactions
- Cards
- Loans
- Payments
- Customer Events

## Key Engineering Capabilities

- Metadata-driven ingestion
- Incremental processing
- Medallion Architecture
- Delta Lake
- Data-quality framework
- Data quarantine
- Deduplication
- CDC processing
- SCD Type 2
- Idempotent pipelines
- Star-schema modeling
- Customer 360
- Fraud analytics
- Pipeline auditing
- Monitoring
- Performance optimization
- Databricks workflow orchestration