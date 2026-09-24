# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_latest_dq_results AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT
# MAGIC         table_name,
# MAGIC         total_records,
# MAGIC         valid_records,
# MAGIC         invalid_records,
# MAGIC         dq_pass_rate,
# MAGIC         run_id,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY table_name
# MAGIC             ORDER BY run_id DESC
# MAGIC         ) AS rn
# MAGIC     FROM workspace.finlake_monitoring.dq_results
# MAGIC )
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     total_records,
# MAGIC     valid_records,
# MAGIC     invalid_records,
# MAGIC     dq_pass_rate,
# MAGIC     run_id
# MAGIC FROM ranked
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_latest_dq_results
# MAGIC ORDER BY invalid_records DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_gold_summary AS
# MAGIC
# MAGIC SELECT 'dim_customer' AS table_name, COUNT(*) AS record_count
# MAGIC FROM workspace.finlake_gold.dim_customer
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_account', COUNT(*)
# MAGIC FROM workspace.finlake_gold.dim_account
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_card', COUNT(*)
# MAGIC FROM workspace.finlake_gold.dim_card
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'dim_loan', COUNT(*)
# MAGIC FROM workspace.finlake_gold.dim_loan
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_transaction', COUNT(*)
# MAGIC FROM workspace.finlake_gold.fact_transaction
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_payment', COUNT(*)
# MAGIC FROM workspace.finlake_gold.fact_payment
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fact_event', COUNT(*)
# MAGIC FROM workspace.finlake_gold.fact_event
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'customer_360', COUNT(*)
# MAGIC FROM workspace.finlake_gold.customer_360
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'fraud_analytics', COUNT(*)
# MAGIC FROM workspace.finlake_gold.fraud_analytics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'loan_portfolio', COUNT(*)
# MAGIC FROM workspace.finlake_gold.loan_portfolio
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 'account_kpis', COUNT(*)
# MAGIC FROM workspace.finlake_gold.account_kpis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_gold_summary
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_scd2_summary AS
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_versions,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN is_current THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS current_records,
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN NOT is_current THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS historical_records,
# MAGIC     COUNT(
# MAGIC         DISTINCT CASE
# MAGIC             WHEN effective_from > DATE('2026-08-03')
# MAGIC             THEN customer_id
# MAGIC         END
# MAGIC     ) AS customers_with_changes
# MAGIC FROM workspace.finlake_silver.customer_scd2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_scd2_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE workspace.finlake_monitoring.quarantine_records;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_quarantine_summary AS
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     failed_column,
# MAGIC     failure_reason,
# MAGIC     COUNT(*) AS quarantined_records,
# MAGIC     MIN(quarantine_timestamp) AS first_quarantined_at,
# MAGIC     MAX(quarantine_timestamp) AS last_quarantined_at
# MAGIC FROM workspace.finlake_monitoring.quarantine_records
# MAGIC GROUP BY
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     failed_column,
# MAGIC     failure_reason;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_quarantine_summary
# MAGIC ORDER BY quarantined_records DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VIEWS IN workspace.finlake_monitoring;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS dq_domains
# MAGIC FROM workspace.finlake_monitoring.v_latest_dq_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS gold_tables
# MAGIC FROM workspace.finlake_monitoring.v_gold_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_scd2_summary;