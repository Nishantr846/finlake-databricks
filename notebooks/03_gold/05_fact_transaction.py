# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Gold Transaction Fact
# MAGIC
# MAGIC **Grain:** one row per transaction.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.finlake_silver.transactions"
TARGET_TABLE = "workspace.finlake_gold.fact_transaction"

tx = spark.table(SOURCE_TABLE)
cust = spark.table("workspace.finlake_gold.dim_customer").filter("is_current = true")
acct = spark.table("workspace.finlake_gold.dim_account")
fact = (tx.alias("t")
    .join(cust.select("customer_id","customer_key").alias("c"), "customer_id", "inner")
    .join(acct.select("account_id","account_key").alias("a"), "account_id", "inner")
    .select(
        "t.transaction_id","c.customer_key","t.customer_id",
        "a.account_key","t.account_id",
        "t.transaction_date","t.transaction_type","t.transaction_status",
        "t.amount","t.merchant","t.transaction_category",
        "t._batch_date"
    ))

fact.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true"
).saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

r = spark.sql("""
SELECT COUNT(*) rows,
       COUNT(DISTINCT transaction_id) distinct_rows,
       SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) missing_customer,
       SUM(CASE WHEN account_key IS NULL THEN 1 ELSE 0 END) missing_account
FROM workspace.finlake_gold.fact_transaction
""").first()
print(r)
assert r["rows"] == r["distinct_rows"]
assert r["missing_customer"] == 0
assert r["missing_account"] == 0
