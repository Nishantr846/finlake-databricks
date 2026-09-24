-- Databricks notebook source
-- MAGIC %md
-- MAGIC # FinLake — Phase 9.6: Databricks SQL Dashboard
-- MAGIC
-- MAGIC This notebook prepares and validates the query layer for the final Databricks SQL dashboard. Create the visual dashboard itself in the Databricks SQL UI.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Executive KPI cards

-- COMMAND ----------

SELECT * FROM workspace.finlake_monitoring.v_executive_kpis;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pipeline health

-- COMMAND ----------

SELECT * FROM workspace.finlake_monitoring.v_pipeline_health;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task health

-- COMMAND ----------

SELECT * FROM workspace.finlake_monitoring.v_task_health ORDER BY started_at;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DQ overview

-- COMMAND ----------

SELECT * FROM workspace.finlake_monitoring.v_dq_overview;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DQ by table

-- COMMAND ----------

SELECT table_name, total_records, valid_records, invalid_records, dq_pass_rate
FROM workspace.finlake_monitoring.v_latest_dq_results
ORDER BY dq_pass_rate ASC;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Top quarantine failures

-- COMMAND ----------

SELECT table_name, rule_id, failed_column, failure_reason, quarantined_records
FROM workspace.finlake_monitoring.v_top_dq_failures
LIMIT 20;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SCD2 health

-- COMMAND ----------

SELECT * FROM workspace.finlake_monitoring.v_scd2_health;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Monthly transaction trend

-- COMMAND ----------

SELECT transaction_month, transaction_count, transaction_value, average_transaction_value
FROM workspace.finlake_monitoring.v_monthly_transaction_trend
ORDER BY transaction_month;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold table volumes

-- COMMAND ----------

SELECT * FROM workspace.finlake_monitoring.v_gold_summary ORDER BY table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Recommended dashboard layout
-- MAGIC
-- MAGIC **Row 1 — Executive KPIs**
-- MAGIC - Total Customers
-- MAGIC - Total Accounts
-- MAGIC - Total Balance
-- MAGIC - Transaction Value
-- MAGIC - Loan Outstanding
-- MAGIC - Suspicious Transaction Value
-- MAGIC
-- MAGIC **Row 2 — Business Trends**
-- MAGIC - Monthly Transaction Value — line chart
-- MAGIC - Monthly Transaction Count — line/bar chart
-- MAGIC - Loan Principal vs Outstanding — bar chart
-- MAGIC
-- MAGIC **Row 3 — Data Engineering Health**
-- MAGIC - Pipeline status
-- MAGIC - Task success rate
-- MAGIC - DQ pass rate
-- MAGIC - Invalid/quarantined records
-- MAGIC
-- MAGIC **Row 4 — Operational Details**
-- MAGIC - DQ failures by table/rule
-- MAGIC - Gold table record counts
-- MAGIC - SCD2 health
-- MAGIC