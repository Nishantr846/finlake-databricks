# FinLake Architecture

FinLake follows a Medallion Lakehouse architecture implemented using
Databricks, PySpark, Delta Lake, and Unity Catalog.

## End-to-End Flow

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
        ↓
Quarantine
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
Databricks SQL Dashboard