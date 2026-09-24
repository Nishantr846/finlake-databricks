# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Account KPI Mart
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

account = spark.table("workspace.finlake_gold.dim_account")

mart = account.groupBy(
    "branch_code","account_type","account_status"
).agg(
    F.countDistinct("account_id").alias("account_count"),
    F.sum("balance").alias("total_balance"),
    F.avg("balance").alias("average_balance"),
    F.max("balance").alias("maximum_balance"))

TARGET_TABLE = "workspace.finlake_gold.account_kpis"
mart.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true").saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

spark.sql("""
SELECT *
FROM workspace.finlake_gold.account_kpis
ORDER BY total_balance DESC
LIMIT 20
""").show()
