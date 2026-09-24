# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 8 — Pipeline Metadata & Audit
# MAGIC
# MAGIC This notebook creates metadata and audit tables used by the
# MAGIC FinLake Databricks workflow.
# MAGIC
# MAGIC pipeline_config:
# MAGIC     Defines pipeline stages, dependencies, notebook paths and execution order.
# MAGIC
# MAGIC pipeline_audit:
# MAGIC     Stores execution status, timestamps, row counts and error information.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM workspace.finlake_monitoring.pipeline_config;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count
# MAGIC FROM workspace.finlake_monitoring.pipeline_config;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.finlake_monitoring.pipeline_config (
# MAGIC     pipeline_name STRING,
# MAGIC     stage_name STRING,
# MAGIC     task_name STRING,
# MAGIC     task_order INT,
# MAGIC     notebook_path STRING,
# MAGIC     dependency_task STRING,
# MAGIC     active BOOLEAN,
# MAGIC     description STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace.finlake_monitoring.pipeline_config VALUES
# MAGIC
# MAGIC -- =========================================================
# MAGIC -- BRONZE
# MAGIC -- =========================================================
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'bronze',
# MAGIC     'bronze_ingestion',
# MAGIC     1,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/01_bronze/02_bronze_metadata_ingestion',
# MAGIC     NULL,
# MAGIC     TRUE,
# MAGIC     'Metadata-driven Bronze ingestion'
# MAGIC ),
# MAGIC
# MAGIC -- =========================================================
# MAGIC -- SILVER
# MAGIC -- =========================================================
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'silver',
# MAGIC     'silver_customers',
# MAGIC     2,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/02_silver_customers',
# MAGIC     'bronze_ingestion',
# MAGIC     TRUE,
# MAGIC     'Customer Silver transformation'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'silver',
# MAGIC     'silver_accounts',
# MAGIC     2,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/03_silver_accounts',
# MAGIC     'bronze_ingestion',
# MAGIC     TRUE,
# MAGIC     'Account Silver transformation'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'silver',
# MAGIC     'silver_transactions',
# MAGIC     2,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/04_silver_transactions',
# MAGIC     'bronze_ingestion',
# MAGIC     TRUE,
# MAGIC     'Transaction Silver transformation'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'silver',
# MAGIC     'silver_cards',
# MAGIC     2,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/05_silver_cards',
# MAGIC     'bronze_ingestion',
# MAGIC     TRUE,
# MAGIC     'Card Silver transformation'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'silver',
# MAGIC     'silver_loans',
# MAGIC     2,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/06_silver_loans',
# MAGIC     'bronze_ingestion',
# MAGIC     TRUE,
# MAGIC     'Loan Silver transformation'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'silver',
# MAGIC     'silver_payments',
# MAGIC     2,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/07_silver_payments',
# MAGIC     'bronze_ingestion',
# MAGIC     TRUE,
# MAGIC     'Payment Silver transformation'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'silver',
# MAGIC     'silver_events',
# MAGIC     2,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/08_silver_events',
# MAGIC     'bronze_ingestion',
# MAGIC     TRUE,
# MAGIC     'Event Silver transformation'
# MAGIC ),
# MAGIC
# MAGIC -- =========================================================
# MAGIC -- DATA QUALITY
# MAGIC -- =========================================================
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'quality',
# MAGIC     'dq_engine',
# MAGIC     3,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/10_dq_engine',
# MAGIC     'silver_customers',
# MAGIC     TRUE,
# MAGIC     'Metadata-driven data quality validation and quarantine'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'quality',
# MAGIC     'dq_audit',
# MAGIC     3,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/11_dq_audit',
# MAGIC     'dq_engine',
# MAGIC     TRUE,
# MAGIC     'Data quality audit and validation results'
# MAGIC ),
# MAGIC
# MAGIC -- =========================================================
# MAGIC -- CUSTOMER CDC / SCD TYPE 2
# MAGIC -- =========================================================
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'cdc',
# MAGIC     'customer_cdc_detection',
# MAGIC     4,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/12_customer_cdc_detection',
# MAGIC     'dq_audit',
# MAGIC     TRUE,
# MAGIC     'Detect customer inserts, updates and unchanged records'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'cdc',
# MAGIC     'customer_scd2',
# MAGIC     5,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/02_silver/13_customer_scd2',
# MAGIC     'customer_cdc_detection',
# MAGIC     TRUE,
# MAGIC     'Maintain Customer SCD Type 2 history'
# MAGIC ),
# MAGIC
# MAGIC -- =========================================================
# MAGIC -- GOLD DIMENSIONS
# MAGIC -- =========================================================
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_dimensions',
# MAGIC     'dim_customer',
# MAGIC     6,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/01_dim_customer',
# MAGIC     'customer_scd2',
# MAGIC     TRUE,
# MAGIC     'Build Gold customer dimension'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_dimensions',
# MAGIC     'dim_account',
# MAGIC     6,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/02_dim_account',
# MAGIC     'customer_scd2',
# MAGIC     TRUE,
# MAGIC     'Build Gold account dimension'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_dimensions',
# MAGIC     'dim_card',
# MAGIC     6,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/03_dim_card',
# MAGIC     'customer_scd2',
# MAGIC     TRUE,
# MAGIC     'Build Gold card dimension'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_dimensions',
# MAGIC     'dim_loan',
# MAGIC     6,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/04_dim_loan',
# MAGIC     'customer_scd2',
# MAGIC     TRUE,
# MAGIC     'Build Gold loan dimension'
# MAGIC ),
# MAGIC
# MAGIC -- =========================================================
# MAGIC -- GOLD FACTS
# MAGIC -- =========================================================
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_facts',
# MAGIC     'fact_transaction',
# MAGIC     7,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/05_fact_transaction',
# MAGIC     'dim_account',
# MAGIC     TRUE,
# MAGIC     'Build transaction fact'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_facts',
# MAGIC     'fact_payment',
# MAGIC     7,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/06_fact_payment',
# MAGIC     'dim_loan',
# MAGIC     TRUE,
# MAGIC     'Build payment fact'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_facts',
# MAGIC     'fact_event',
# MAGIC     7,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/07_fact_event',
# MAGIC     'dim_customer',
# MAGIC     TRUE,
# MAGIC     'Build event fact'
# MAGIC ),
# MAGIC
# MAGIC -- =========================================================
# MAGIC -- BUSINESS MARTS
# MAGIC -- =========================================================
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_marts',
# MAGIC     'customer_360',
# MAGIC     8,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/08_customer_360',
# MAGIC     'fact_event',
# MAGIC     TRUE,
# MAGIC     'Build Customer 360 analytical mart'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_marts',
# MAGIC     'fraud_analytics',
# MAGIC     8,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/09_fraud_analytics',
# MAGIC     'fact_transaction',
# MAGIC     TRUE,
# MAGIC     'Build fraud analytics mart'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_marts',
# MAGIC     'loan_portfolio',
# MAGIC     8,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/10_loan_portfolio',
# MAGIC     'fact_payment',
# MAGIC     TRUE,
# MAGIC     'Build loan portfolio mart'
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'finlake_daily',
# MAGIC     'gold_marts',
# MAGIC     'account_kpis',
# MAGIC     8,
# MAGIC     '/Workspace/Users/nishantr846@gmail.com/finlake/03_gold/11_account_kpis',
# MAGIC     'dim_account',
# MAGIC     TRUE,
# MAGIC     'Build account KPI mart'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS workspace.finlake_monitoring.pipeline_audit (
# MAGIC     pipeline_run_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     task_name STRING,
# MAGIC     stage_name STRING,
# MAGIC
# MAGIC     run_date DATE,
# MAGIC
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC
# MAGIC     status STRING,
# MAGIC
# MAGIC     input_rows BIGINT,
# MAGIC     output_rows BIGINT,
# MAGIC
# MAGIC     error_message STRING,
# MAGIC
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.finlake_monitoring;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.pipeline_config
# MAGIC ORDER BY task_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.finlake_monitoring.pipeline_audit;

# COMMAND ----------

SCD_TABLE = "workspace.finlake_silver.customer_scd2"
# spark.sql(f"DROP TABLE IF EXISTS {SCD_TABLE}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_versions,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_records,
# MAGIC     SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS expired_records
# MAGIC FROM workspace.finlake_silver.customer_scd2;

# COMMAND ----------

