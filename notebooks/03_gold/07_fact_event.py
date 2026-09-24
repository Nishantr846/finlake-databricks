# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Gold Event Fact
# MAGIC
# MAGIC **Grain:** one row per customer event.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.finlake_silver.events"
TARGET_TABLE = "workspace.finlake_gold.fact_event"

e = spark.table(SOURCE_TABLE)
cust = spark.table("workspace.finlake_gold.dim_customer").filter("is_current = true")

fact = (e.alias("e")
    .join(cust.select("customer_id","customer_key").alias("c"), "customer_id", "inner")
    .select(
        "e.event_id","c.customer_key","e.customer_id","e.event_type",
        "e.event_timestamp","e.ip_address","e.browser","e.transfer_type",
        "e.payment_type","e.loan_type","e.card_type","e.field_updated",
        "e.event_metadata_amount","e._batch_date"
    ))

fact.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true"
).saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

r = spark.sql("""
SELECT COUNT(*) rows,
       COUNT(DISTINCT event_id) distinct_rows,
       SUM(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) missing_customer
FROM workspace.finlake_gold.fact_event
""").first()
print(r)
assert r["rows"] == r["distinct_rows"]
assert r["missing_customer"] == 0


# COMMAND ----------

