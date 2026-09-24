# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE job_id = '916334386166307'
# MAGIC ORDER BY period_start_time DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_pipeline_health AS
# MAGIC
# MAGIC WITH job_runs AS (
# MAGIC     SELECT
# MAGIC         job_id,
# MAGIC         run_id,
# MAGIC         MIN(period_start_time) AS started_at,
# MAGIC         MAX(period_end_time) AS ended_at,
# MAGIC         MAX(result_state) AS result_state,
# MAGIC         MAX(termination_code) AS termination_code,
# MAGIC         MAX(trigger_type) AS trigger_type,
# MAGIC         MAX(run_type) AS run_type
# MAGIC     FROM system.lakeflow.job_run_timeline
# MAGIC     WHERE job_id = '916334386166307'
# MAGIC     GROUP BY
# MAGIC         job_id,
# MAGIC         run_id
# MAGIC ),
# MAGIC
# MAGIC latest_run AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             ORDER BY started_at DESC
# MAGIC         ) AS rn
# MAGIC     FROM job_runs
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     job_id,
# MAGIC     run_id,
# MAGIC     started_at,
# MAGIC     ended_at,
# MAGIC     ROUND(
# MAGIC         UNIX_TIMESTAMP(ended_at) -
# MAGIC         UNIX_TIMESTAMP(started_at)
# MAGIC     , 2) AS duration_seconds,
# MAGIC     result_state,
# MAGIC     termination_code,
# MAGIC     trigger_type,
# MAGIC     run_type
# MAGIC FROM latest_run
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_pipeline_health;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_task_health AS
# MAGIC
# MAGIC WITH task_runs AS (
# MAGIC     SELECT
# MAGIC         job_id,
# MAGIC         job_run_id,
# MAGIC         task_key,
# MAGIC         MIN(period_start_time) AS started_at,
# MAGIC         MAX(period_end_time) AS ended_at,
# MAGIC         MAX(result_state) AS result_state,
# MAGIC         MAX(termination_code) AS termination_code
# MAGIC     FROM system.lakeflow.job_task_run_timeline
# MAGIC     WHERE job_id = '916334386166307'
# MAGIC     GROUP BY
# MAGIC         job_id,
# MAGIC         job_run_id,
# MAGIC         task_key
# MAGIC ),
# MAGIC
# MAGIC latest_job_run AS (
# MAGIC     SELECT
# MAGIC         job_run_id
# MAGIC     FROM task_runs
# MAGIC     ORDER BY started_at DESC
# MAGIC     LIMIT 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     t.task_key,
# MAGIC     t.started_at,
# MAGIC     t.ended_at,
# MAGIC     ROUND(
# MAGIC         UNIX_TIMESTAMP(t.ended_at) -
# MAGIC         UNIX_TIMESTAMP(t.started_at)
# MAGIC     , 2) AS duration_seconds,
# MAGIC     t.result_state,
# MAGIC     t.termination_code
# MAGIC FROM task_runs t
# MAGIC INNER JOIN latest_job_run r
# MAGIC     ON t.job_run_id = r.job_run_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_task_health
# MAGIC ORDER BY started_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.finlake_monitoring.v_pipeline_kpis AS
# MAGIC
# MAGIC WITH task_health AS (
# MAGIC     SELECT *
# MAGIC     FROM workspace.finlake_monitoring.v_task_health
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_tasks,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN result_state = 'SUCCEEDED'
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS successful_tasks,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN result_state IN ('FAILED', 'ERROR')
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS failed_tasks,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN result_state = 'SKIPPED'
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS skipped_tasks,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 *
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN result_state = 'SUCCEEDED'
# MAGIC                 THEN 1
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) / COUNT(*)
# MAGIC     , 2) AS task_success_rate,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(duration_seconds)
# MAGIC     , 2) AS total_task_runtime_seconds,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(duration_seconds)
# MAGIC     , 2) AS average_task_runtime_seconds,
# MAGIC
# MAGIC     MAX(duration_seconds) AS longest_task_seconds
# MAGIC
# MAGIC FROM task_health;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.v_pipeline_kpis;

# COMMAND ----------

