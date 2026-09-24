# FinLake — Metadata-Driven Banking Lakehouse

> An end-to-end banking data platform built with Databricks, PySpark, Delta Lake, Unity Catalog, Auto Loader, CDC, SCD Type 2, data quality monitoring, and Databricks AI/BI.

![FinLake Architecture]![complete workflow](<screenshots/architecture/complete workflow.png>)


---

## 📌 Overview

**FinLake** is a metadata-driven banking lakehouse designed to demonstrate production-style data engineering concepts using Databricks.

The platform processes synthetic banking data through a complete **Bronze → Silver → Gold** architecture and includes:

- Metadata-driven ingestion
- Incremental processing
- Auto Loader
- Delta Lake
- Unity Catalog
- Data quality validation
- Data quarantine
- Customer CDC
- SCD Type 2
- Idempotent processing
- Star schema modeling
- Business analytics
- Databricks Jobs orchestration
- Pipeline monitoring
- Databricks Lakeflow system tables
- Databricks AI/BI dashboard

The project was developed using **Databricks Free Edition**, local Python, and GitHub without requiring paid cloud infrastructure.

---

# 🏗️ Architecture

```text
Synthetic Banking Data
        ↓
Databricks Volume
        ↓
     BRONZE
        ↓
     SILVER
        ↓
Data Quality
   ↙          ↘
Valid       Quarantine
   ↓
Customer CDC
   ↓
Customer SCD2
   ↓
     GOLD
   ↓
Business Marts
   ↓
Monitoring
   ↓
Databricks AI/BI Dashboard