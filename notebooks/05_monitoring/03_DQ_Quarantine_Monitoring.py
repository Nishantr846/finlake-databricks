# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_latest_dq_results
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_quarantine_summary
# MAGIC ORDER BY quarantined_records DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_dq_overview AS
# MAGIC
# MAGIC WITH dq AS (
# MAGIC     SELECT *
# MAGIC     FROM workspace.finlake_monitoring.v_latest_dq_results
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_tables,
# MAGIC
# MAGIC     SUM(total_records) AS total_records,
# MAGIC
# MAGIC     SUM(valid_records) AS valid_records,
# MAGIC
# MAGIC     SUM(invalid_records) AS invalid_records,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * SUM(valid_records) / SUM(total_records),
# MAGIC         2
# MAGIC     ) AS overall_dq_pass_rate,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN invalid_records > 0 THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS tables_with_violations,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN invalid_records = 0 THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS tables_without_violations
# MAGIC
# MAGIC FROM dq;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_dq_overview;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_top_dq_failures AS
# MAGIC
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     failed_column,
# MAGIC     failure_reason,
# MAGIC     quarantined_records,
# MAGIC     first_quarantined_at,
# MAGIC     last_quarantined_at
# MAGIC FROM workspace.finlake_monitoring.v_quarantine_summary
# MAGIC ORDER BY quarantined_records DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_top_dq_failures
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_dq_overview;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     total_records,
# MAGIC     valid_records,
# MAGIC     invalid_records,
# MAGIC     dq_pass_rate
# MAGIC FROM workspace.finlake_monitoring.v_latest_dq_results
# MAGIC ORDER BY dq_pass_rate ASC;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     failed_column,
# MAGIC     quarantined_records,
# MAGIC     failure_reason
# MAGIC FROM workspace.finlake_monitoring.v_top_dq_failures
# MAGIC LIMIT 20;

# COMMAND ----------

