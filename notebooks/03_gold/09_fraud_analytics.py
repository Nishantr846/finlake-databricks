# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Fraud Analytics Mart
# MAGIC
# MAGIC Analytical screening mart; not a production fraud model.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

tx = spark.table("workspace.finlake_gold.fact_transaction")
cust = spark.table("workspace.finlake_gold.dim_customer").filter("is_current = true")

mart = (tx.join(
    cust.select("customer_id","customer_segment","city","customer_status"),
    "customer_id","left")
    .withColumn("high_value_flag", F.when(F.col("amount") >= 50000,1).otherwise(0))
    .withColumn("failed_transaction_flag",
                F.when(F.upper("transaction_status").isin("FAILED","DECLINED"),1).otherwise(0))
    .withColumn("fraud_review_flag",
                F.when((F.col("high_value_flag")==1)|(F.col("failed_transaction_flag")==1),1).otherwise(0)))

TARGET_TABLE = "workspace.finlake_gold.fraud_analytics"
mart.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true").saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

spark.sql("""
SELECT COUNT(*) transactions,
       SUM(high_value_flag) high_value_transactions,
       SUM(failed_transaction_flag) failed_transactions,
       SUM(fraud_review_flag) review_candidates
FROM workspace.finlake_gold.fraud_analytics
""").show()
