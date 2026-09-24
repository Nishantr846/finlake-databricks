# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Gold Payment Fact
# MAGIC
# MAGIC **Grain:** one row per payment.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.finlake_silver.payments"
TARGET_TABLE = "workspace.finlake_gold.fact_payment"

p = spark.table(SOURCE_TABLE)
cust = spark.table("workspace.finlake_gold.dim_customer").filter("is_current = true")
loan = spark.table("workspace.finlake_gold.dim_loan")

fact = (p.alias("p")
    .join(cust.select("customer_id","customer_key").alias("c"), "customer_id", "left")
    .join(loan.select("loan_id","loan_key").alias("l"), "loan_id", "left")
    .select(
        "p.payment_id","c.customer_key","p.customer_id",
        "l.loan_key","p.loan_id","p.payment_date","p.payment_amount",
        "p.payment_method","p.payment_status","p._batch_date"
    ))

fact.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true"
).saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

r = spark.sql("""
SELECT COUNT(*) rows,
       COUNT(DISTINCT payment_id) distinct_rows,
       SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) missing_customer,
       SUM(CASE WHEN loan_key IS NULL THEN 1 ELSE 0 END) missing_loan
FROM workspace.finlake_gold.fact_payment WHERE customer_key IS NOT NULL AND loan_key IS NOT NULL
""").first()
print(r)
assert r["rows"] == r["distinct_rows"]
assert r["missing_customer"] == 0
assert r["missing_loan"] == 0
