# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 5 — Data Quality Engine
# MAGIC
# MAGIC This notebook implements the reusable metadata-driven Data Quality
# MAGIC framework for the FinLake banking lakehouse.
# MAGIC
# MAGIC The engine reads validation rules from the `dq_rules` metadata table,
# MAGIC executes the applicable checks against Silver tables, identifies
# MAGIC invalid records, and writes failures to the quarantine table.
# MAGIC
# MAGIC DQ execution results are recorded in the monitoring layer.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------

DQ_RULES_TABLE = "workspace.finlake_monitoring.dq_rules"
DQ_RESULTS_TABLE = "workspace.finlake_monitoring.dq_results"
QUARANTINE_TABLE = "workspace.finlake_monitoring.quarantine_records"

SILVER_SCHEMA = "workspace.finlake_silver"

RUN_ID = str(uuid.uuid4())

print(f"DQ Run ID: {RUN_ID}")

# COMMAND ----------

rules_df = (
    spark.table(DQ_RULES_TABLE)
    .filter(F.col("active") == True)
)

display(rules_df)

# COMMAND ----------

rules = rules_df.collect()

print(f"Active DQ rules: {len(rules)}")

# COMMAND ----------

def get_silver_table(table_name):
    return spark.table(f"{SILVER_SCHEMA}.{table_name}")

# COMMAND ----------

def execute_rule(rule, df):
    
    rule_id = rule["rule_id"]
    rule_type = rule["rule_type"]
    column = rule["column_name"]
    
    failed_df = None
    
    if rule_type == "NOT_NULL":
        
        failed_df = df.filter(
            F.col(column).isNull()
        )
        
    elif rule_type == "POSITIVE":
        
        failed_df = df.filter(
            F.col(column).isNull() |
            (F.col(column) <= 0)
        )
        
    elif rule_type == "NON_NEGATIVE":
        
        failed_df = df.filter(
            F.col(column).isNull() |
            (F.col(column) < 0)
        )
        
    elif rule_type == "EMAIL_FORMAT":
        
        failed_df = df.filter(
            F.col(column).isNull() |
            (~F.col(column).rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            ))
        )
        
    elif rule_type == "ALLOWED_VALUES":
        
        allowed_values = {
            "customer_status": ["ACTIVE", "INACTIVE", "BLOCKED"]
        }
        
        values = allowed_values.get(column, [])
        
        failed_df = df.filter(
            F.col(column).isNull() |
            (~F.col(column).isin(values))
        )
        
    elif rule_type == "REFERENTIAL":
        
        if rule["table_name"] == "accounts":
            reference_table = "customers"
            reference_column = "customer_id"
            
        elif rule["table_name"] == "transactions":
            
            if column == "account_id":
                reference_table = "accounts"
                reference_column = "account_id"
            else:
                reference_table = "customers"
                reference_column = "customer_id"
                
        elif rule["table_name"] == "cards":
            reference_table = "accounts"
            reference_column = "account_id"
            
        elif rule["table_name"] == "loans":
            reference_table = "customers"
            reference_column = "customer_id"
            
        elif rule["table_name"] == "payments":
            
            if column == "loan_id":
                reference_table = "loans"
                reference_column = "loan_id"
            else:
                reference_table = "customers"
                reference_column = "customer_id"
                
        elif rule["table_name"] == "events":
            reference_table = "customers"
            reference_column = "customer_id"
            
        reference_df = (
            spark.table(f"{SILVER_SCHEMA}.{reference_table}")
            .select(reference_column)
            .dropDuplicates()
        )
        
        failed_df = (
            df.join(
                reference_df,
                df[column] == reference_df[reference_column],
                "left_anti"
            )
        )
        
    elif rule_type == "RANGE":
        
        if column == "outstanding_amount":
            
            failed_df = df.filter(
                F.col(column).isNull() |
                (F.col(column) < 0) |
                (F.col(column) > F.col("principal_amount"))
            )
            
    elif rule_type == "DATE_AFTER":
        
        if column == "expiry_date":
            
            failed_df = df.filter(
                F.col("expiry_date").isNull() |
                F.col("issue_date").isNull() |
                (F.col("expiry_date") <= F.col("issue_date"))
            )
    
    else:
        print(f"Unsupported rule type: {rule_type}")
        return None
    
    return failed_df

# COMMAND ----------

allowed_values = {
    "customer_status": ["ACTIVE", "INACTIVE", "BLOCKED"]
}

# COMMAND ----------

quarantine_results = []
dq_summary = []

for rule in rules:
    
    table_name = rule["table_name"]
    
    print(
        f"Running {rule['rule_id']} "
        f"on {table_name}.{rule['column_name']}"
    )
    
    df = get_silver_table(table_name)
    
    total_records = df.count()
    
    failed_df = execute_rule(rule, df)
    
    if failed_df is None:
        continue
    
    failed_count = failed_df.count()
    
    print(f"Failed records: {failed_count}")
    
    if failed_count > 0:
        
        record_id_column = {
            "customers": "customer_id",
            "accounts": "account_id",
            "transactions": "transaction_id",
            "cards": "card_id",
            "loans": "loan_id",
            "payments": "payment_id",
            "events": "event_id"
        }[table_name]
        
        quarantine_df = (
            failed_df
            .select(
                F.lit(RUN_ID).alias("run_id"),
                F.lit(table_name).alias("table_name"),
                F.col(record_id_column).cast("string").alias("record_id"),
                F.lit(rule["rule_id"]).alias("rule_id"),
                F.lit(rule["column_name"]).alias("failed_column"),
                F.lit(rule["rule_description"]).alias("failure_reason"),
                F.current_timestamp().alias("quarantine_timestamp")
            )
        )
        
        quarantine_results.append(quarantine_df)

# COMMAND ----------

if quarantine_results:
    
    final_quarantine_df = quarantine_results[0]
    
    for qdf in quarantine_results[1:]:
        final_quarantine_df = final_quarantine_df.unionByName(qdf)
    
    (
        final_quarantine_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(QUARANTINE_TABLE)
    )
    
    print("Quarantine records written successfully.")
    
else:
    print("No DQ failures detected.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     failed_column,
# MAGIC     COUNT(*) AS failed_records
# MAGIC FROM workspace.finlake_monitoring.quarantine_records
# MAGIC GROUP BY
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     failed_column
# MAGIC ORDER BY
# MAGIC     table_name,
# MAGIC     rule_id;

# COMMAND ----------

print("DQ engine execution completed.")
print(f"Run ID: {RUN_ID}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_quarantine_records
# MAGIC FROM workspace.finlake_monitoring.quarantine_records;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.finlake_monitoring.quarantine_records
# MAGIC ORDER BY quarantine_timestamp DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     failed_column,
# MAGIC     COUNT(*) AS failed_records
# MAGIC FROM workspace.finlake_monitoring.quarantine_records
# MAGIC GROUP BY
# MAGIC     table_name,
# MAGIC     rule_id,
# MAGIC     failed_column
# MAGIC ORDER BY
# MAGIC     failed_records DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     COUNT(*) AS quarantine_records
# MAGIC FROM workspace.finlake_monitoring.quarantine_records
# MAGIC GROUP BY run_id
# MAGIC ORDER BY quarantine_records DESC;

# COMMAND ----------

