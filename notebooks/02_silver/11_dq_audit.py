# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — Data Quality Audit
# MAGIC
# MAGIC This notebook generates execution-level Data Quality metrics for the
# MAGIC FinLake Silver layer.
# MAGIC
# MAGIC The audit captures:
# MAGIC
# MAGIC - Total records evaluated
# MAGIC - Unique invalid records
# MAGIC - Valid records
# MAGIC - DQ pass rate
# MAGIC - Number of active rules executed
# MAGIC - DQ execution timestamp
# MAGIC
# MAGIC Invalid records are counted using distinct business keys so that a
# MAGIC record failing multiple rules is counted only once.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

DQ_RULES_TABLE = "workspace.finlake_monitoring.dq_rules"
DQ_RESULTS_TABLE = "workspace.finlake_monitoring.dq_results"
QUARANTINE_TABLE = "workspace.finlake_monitoring.quarantine_records"

SILVER_SCHEMA = "workspace.finlake_silver"

TABLE_KEYS = {
    "customers": "customer_id",
    "accounts": "account_id",
    "transactions": "transaction_id",
    "cards": "card_id",
    "loans": "loan_id",
    "payments": "payment_id",
    "events": "event_id"
}

# COMMAND ----------

latest_run = (
    spark.table(QUARANTINE_TABLE)
    .select("run_id")
    .orderBy(F.col("quarantine_timestamp").desc())
    .limit(1)
    .collect()
)

if latest_run:
    RUN_ID = latest_run[0]["run_id"]
else:
    RUN_ID = None

print(f"Latest DQ Run ID: {RUN_ID}")

# COMMAND ----------

audit_rows = []

rules_df = (
    spark.table(DQ_RULES_TABLE)
    .filter(F.col("active") == True)
)

for table_name, key_column in TABLE_KEYS.items():

    silver_df = spark.table(
        f"{SILVER_SCHEMA}.{table_name}"
    )

    total_records = silver_df.count()

    if RUN_ID:

        invalid_records = (
            spark.table(QUARANTINE_TABLE)
            .filter(
                (F.col("run_id") == RUN_ID) &
                (F.col("table_name") == table_name)
            )
            .select("record_id")
            .distinct()
            .count()
        )

    else:
        invalid_records = 0

    valid_records = total_records - invalid_records

    dq_pass_rate = (
        valid_records / total_records * 100
        if total_records > 0
        else 0
    )

    rules_executed = (
        rules_df
        .filter(F.col("table_name") == table_name)
        .count()
    )

    audit_rows.append(
        (
            RUN_ID,
            table_name,
            total_records,
            valid_records,
            invalid_records,
            round(dq_pass_rate, 4),
            rules_executed,
            datetime.now()
        )
    )

# COMMAND ----------

audit_df = spark.createDataFrame(
    audit_rows,
    [
        "run_id",
        "table_name",
        "total_records",
        "valid_records",
        "invalid_records",
        "dq_pass_rate",
        "rules_executed",
        "execution_timestamp"
    ]
)

display(audit_df.orderBy("table_name"))

# COMMAND ----------

(
    audit_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(DQ_RESULTS_TABLE)
)

print("DQ audit results written successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     total_records,
# MAGIC     valid_records,
# MAGIC     invalid_records,
# MAGIC     dq_pass_rate,
# MAGIC     rules_executed,
# MAGIC     execution_timestamp
# MAGIC FROM workspace.finlake_monitoring.dq_results
# MAGIC ORDER BY table_name;