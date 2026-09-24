# FinLake Architecture

## Overview

FinLake is a metadata-driven banking lakehouse built using Databricks,
PySpark, Delta Lake, Unity Catalog, and Databricks SQL.

The platform demonstrates an end-to-end data engineering workflow:

Synthetic Source Data
→ Incremental Ingestion
→ Bronze
→ Silver
→ Data Quality / Quarantine
→ Customer CDC / SCD Type 2
→ Gold
→ Business Marts
→ Monitoring
→ Databricks SQL Dashboard

## Technology Stack

| Layer | Technology |
|---|---|
| Data Generation | Python, Pandas, NumPy, Faker |
| Processing | PySpark |
| Lakehouse | Databricks, Delta Lake |
| Governance | Unity Catalog |
| Ingestion | Auto Loader |
| Orchestration | Databricks Jobs |
| Data Quality | Metadata-driven DQ framework |
| CDC | Simulated Customer CDC |
| Historical Tracking | SCD Type 2 |
| Analytics | Databricks SQL |
| Version Control | Git, GitHub |

## Medallion Architecture

### Bronze

Bronze contains raw ingested source data stored as Delta tables.

The ingestion layer is metadata-driven using:

`workspace.finlake_monitoring.ingestion_config`

Metadata captured during ingestion includes:

- `_ingest_timestamp`
- `_source_file`
- `_batch_date`

### Silver

Silver contains cleaned and standardized datasets.

Processing includes:

- data type standardization
- trimming and normalization
- deduplication
- referential integrity validation
- domain-specific validation
- event metadata parsing

Silver domains:

- Customers
- Accounts
- Transactions
- Cards
- Loans
- Payments
- Events

### Gold

Gold contains analytics-ready dimensions, facts, and business marts.

### Dimensions

- `dim_customer`
- `dim_account`
- `dim_card`
- `dim_loan`

### Facts

- `fact_transaction`
- `fact_payment`
- `fact_event`

### Business Marts

- `customer_360`
- `fraud_analytics`
- `loan_portfolio`
- `account_kpis`

## Data Quality

Data quality rules are maintained in:

`workspace.finlake_monitoring.dq_rules`

Invalid records are written to:

`workspace.finlake_monitoring.quarantine_records`

The DQ framework validates the seven Silver domains before trusted data is consumed by the Gold layer.

## Customer CDC and SCD Type 2

Customer CDC is implemented separately from the standard ingestion pipeline.

The CDC batch contains:

- 500 inserts
- 500 updates
- 9,500 unchanged records

SCD Type 2 is implemented in:

`workspace.finlake_silver.customer_scd2`

Validated state:

- 10,500 unique customers
- 11,000 total versions
- 10,500 current records
- 500 historical records

CDC/SCD2 is intentionally limited to Customers.

## Orchestration

The main Databricks workflow is:

`Finlake_Daily_Pipeline`

The workflow contains 23 tasks.

High-level dependency flow:

Bronze
→ Silver
→ Data Quality
→ CDC Detection
→ Customer SCD2
→ Gold Dimensions
→ Gold Facts
→ Gold Business Marts

The validated pipeline run achieved:

- 23 successful tasks
- 0 failed tasks
- 0 skipped tasks
- 100% task success rate

## Monitoring

Monitoring is implemented using project-specific monitoring views and Databricks Lakeflow system tables.

Monitoring includes:

- pipeline health
- task health
- DQ pass rates
- quarantine failures
- SCD2 integrity
- Gold table volumes
- business KPIs
- final platform health

## Design Principles

### Metadata-driven processing

Configuration is separated from processing logic wherever practical.

### Idempotency

The Customer SCD2 pipeline was tested by replaying the same CDC batch and confirmed that duplicate versions were not created.

### Separation of concerns

Bronze, Silver, Gold, monitoring, and data-generation responsibilities are separated.

### Controlled scope

Customer CDC/SCD2 demonstrates the historical-dimension pattern without unnecessarily applying it to every domain.

## Scope

FinLake uses synthetic banking data and simulated CDC.

It is a portfolio data engineering platform and does not represent a production banking system.