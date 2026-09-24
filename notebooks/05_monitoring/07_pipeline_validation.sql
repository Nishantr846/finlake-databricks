-- Databricks notebook source
SELECT *
FROM workspace.finlake_monitoring.v_pipeline_health;

-- COMMAND ----------

SELECT *
FROM workspace.finlake_monitoring.v_pipeline_kpis;

-- COMMAND ----------

SELECT
    task_key,
    result_state,
    duration_seconds
FROM workspace.finlake_monitoring.v_task_health
ORDER BY started_at;

-- COMMAND ----------

SELECT *
FROM workspace.finlake_monitoring.v_dq_overview;

-- COMMAND ----------

SELECT
    SUM(total_records) AS total_records,
    SUM(valid_records) AS valid_records,
    SUM(invalid_records) AS invalid_records
FROM workspace.finlake_monitoring.v_latest_dq_results;

-- COMMAND ----------

SELECT *
FROM workspace.finlake_monitoring.v_scd2_health;

-- COMMAND ----------

SELECT
    COUNT(*) AS total_versions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_records,
    SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) AS historical_records
FROM workspace.finlake_silver.customer_scd2;

-- COMMAND ----------

SELECT *
FROM workspace.finlake_monitoring.v_gold_summary
ORDER BY table_name;

-- COMMAND ----------

SELECT
    COUNT(*) AS transaction_count
FROM workspace.finlake_gold.fact_transaction;

-- COMMAND ----------

SELECT
    COUNT(*) AS payment_count
FROM workspace.finlake_gold.fact_payment;

-- COMMAND ----------

SELECT
    COUNT(*) AS event_count
FROM workspace.finlake_gold.fact_event;

-- COMMAND ----------

SELECT
    COUNT(*) AS customer_360_count
FROM workspace.finlake_gold.customer_360;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_finlake_health AS

WITH pipeline AS (
    SELECT *
    FROM workspace.finlake_monitoring.v_pipeline_kpis
),

dq AS (
    SELECT *
    FROM workspace.finlake_monitoring.v_dq_overview
),

scd AS (
    SELECT *
    FROM workspace.finlake_monitoring.v_scd2_health
),

gold AS (
    SELECT
        COUNT(*) AS gold_tables
    FROM workspace.finlake_monitoring.v_gold_summary
)

SELECT
    pipeline.total_tasks,
    pipeline.successful_tasks,
    pipeline.failed_tasks,
    pipeline.task_success_rate,

    dq.total_tables AS dq_tables,
    dq.total_records AS dq_total_records,
    dq.invalid_records AS dq_invalid_records,
    dq.overall_dq_pass_rate,

    scd.total_versions AS scd2_versions,
    scd.unique_customers AS scd2_customers,
    scd.current_records AS scd2_current_records,
    scd.historical_records AS scd2_historical_records,
    scd.scd2_health_status,

    gold.gold_tables,

    CASE
        WHEN pipeline.failed_tasks = 0
         AND pipeline.skipped_tasks = 0
         AND dq.overall_dq_pass_rate >= 99
         AND scd.scd2_health_status = 'HEALTHY'
        THEN 'HEALTHY'
        ELSE 'CHECK_REQUIRED'
    END AS overall_health_status

FROM pipeline
CROSS JOIN dq
CROSS JOIN scd
CROSS JOIN gold;

-- COMMAND ----------

SELECT *
FROM workspace.finlake_monitoring.v_finlake_health;

-- COMMAND ----------

SELECT *
FROM workspace.finlake_monitoring.v_finlake_health;