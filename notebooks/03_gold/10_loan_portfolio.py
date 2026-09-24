# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Loan Portfolio Mart
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

loan = spark.table("workspace.finlake_gold.dim_loan")
cust = spark.table("workspace.finlake_gold.dim_customer").filter("is_current = true")

mart = (loan.join(
    cust.select("customer_id","customer_segment","city"),
    "customer_id","left")
    .withColumn("outstanding_ratio",
                F.when(F.col("principal_amount") > 0,
                       F.col("outstanding_amount")/F.col("principal_amount")).otherwise(0))
    .withColumn("high_exposure_flag",
                F.when(F.col("outstanding_amount") >= 1000000,1).otherwise(0)))

TARGET_TABLE = "workspace.finlake_gold.loan_portfolio"
mart.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true").saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

spark.sql("""
SELECT COUNT(*) loans,
       SUM(principal_amount) total_principal,
       SUM(outstanding_amount) total_outstanding,
       AVG(outstanding_ratio) avg_outstanding_ratio,
       SUM(high_exposure_flag) high_exposure_loans
FROM workspace.finlake_gold.loan_portfolio
""").show()
