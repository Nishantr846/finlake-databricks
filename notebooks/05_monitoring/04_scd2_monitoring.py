# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_scd2_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_scd2_health AS
# MAGIC
# MAGIC WITH summary AS (
# MAGIC     SELECT *
# MAGIC     FROM workspace.finlake_monitoring.v_scd2_summary
# MAGIC ),
# MAGIC
# MAGIC duplicate_current AS (
# MAGIC     SELECT COUNT(*) AS duplicate_current_customers
# MAGIC     FROM (
# MAGIC         SELECT customer_id
# MAGIC         FROM workspace.finlake_silver.customer_scd2
# MAGIC         WHERE is_current = TRUE
# MAGIC         GROUP BY customer_id
# MAGIC         HAVING COUNT(*) > 1
# MAGIC     )
# MAGIC ),
# MAGIC
# MAGIC missing_current AS (
# MAGIC     SELECT COUNT(*) AS customers_without_current
# MAGIC     FROM (
# MAGIC         SELECT customer_id
# MAGIC         FROM workspace.finlake_silver.customer_scd2
# MAGIC         GROUP BY customer_id
# MAGIC         HAVING SUM(CASE WHEN is_current = TRUE THEN 1 ELSE 0 END) = 0
# MAGIC     )
# MAGIC ),
# MAGIC
# MAGIC invalid_dates AS (
# MAGIC     SELECT COUNT(*) AS invalid_date_records
# MAGIC     FROM workspace.finlake_silver.customer_scd2
# MAGIC     WHERE effective_from >= effective_to
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     summary.total_versions,
# MAGIC     summary.unique_customers,
# MAGIC     summary.current_records,
# MAGIC     summary.historical_records,
# MAGIC     summary.customers_with_changes,
# MAGIC
# MAGIC     duplicate_current.duplicate_current_customers,
# MAGIC     missing_current.customers_without_current,
# MAGIC     invalid_dates.invalid_date_records,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN duplicate_current.duplicate_current_customers = 0
# MAGIC          AND missing_current.customers_without_current = 0
# MAGIC          AND invalid_dates.invalid_date_records = 0
# MAGIC         THEN 'HEALTHY'
# MAGIC         ELSE 'CHECK_REQUIRED'
# MAGIC     END AS scd2_health_status
# MAGIC
# MAGIC FROM summary
# MAGIC CROSS JOIN duplicate_current
# MAGIC CROSS JOIN missing_current
# MAGIC CROSS JOIN invalid_dates;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_scd2_health;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN version_count = 1 THEN '1 version'
# MAGIC         WHEN version_count = 2 THEN '2 versions'
# MAGIC         ELSE '3+ versions'
# MAGIC     END AS version_category,
# MAGIC     COUNT(*) AS customer_count
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(*) AS version_count
# MAGIC     FROM workspace.finlake_silver.customer_scd2
# MAGIC     GROUP BY customer_id
# MAGIC )
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN version_count = 1 THEN '1 version'
# MAGIC         WHEN version_count = 2 THEN '2 versions'
# MAGIC         ELSE '3+ versions'
# MAGIC     END
# MAGIC ORDER BY version_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_scd2_health;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN version_count = 1 THEN '1 version'
# MAGIC         WHEN version_count = 2 THEN '2 versions'
# MAGIC         ELSE '3+ versions'
# MAGIC     END AS version_category,
# MAGIC     COUNT(*) AS customer_count
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         customer_id,
# MAGIC         COUNT(*) AS version_count
# MAGIC     FROM workspace.finlake_silver.customer_scd2
# MAGIC     GROUP BY customer_id
# MAGIC )
# MAGIC GROUP BY
# MAGIC     CASE
# MAGIC         WHEN version_count = 1 THEN '1 version'
# MAGIC         WHEN version_count = 2 THEN '2 versions'
# MAGIC         ELSE '3+ versions'
# MAGIC     END
# MAGIC ORDER BY version_category;