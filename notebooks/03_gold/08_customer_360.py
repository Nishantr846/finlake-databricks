# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 7 — Customer 360 Mart
# MAGIC
# MAGIC Create a customer-level analytical mart.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

customer = spark.table("workspace.finlake_gold.dim_customer").filter("is_current = true")
account = spark.table("workspace.finlake_gold.dim_account")
tx = spark.table("workspace.finlake_gold.fact_transaction")
loan = spark.table("workspace.finlake_gold.dim_loan")
payment = spark.table("workspace.finlake_gold.fact_payment")
event = spark.table("workspace.finlake_gold.fact_event")

a = account.groupBy("customer_id").agg(
    F.countDistinct("account_id").alias("account_count"),
    F.sum("balance").alias("total_account_balance"))

t = tx.groupBy("customer_id").agg(
    F.countDistinct("transaction_id").alias("transaction_count"),
    F.sum(F.when(F.upper("transaction_status")=="SUCCESS",F.col("amount")).otherwise(0)).alias("successful_transaction_amount"),
    F.avg("amount").alias("avg_transaction_amount"))

l = loan.groupBy("customer_id").agg(
    F.countDistinct("loan_id").alias("loan_count"),
    F.sum("principal_amount").alias("total_loan_principal"),
    F.sum("outstanding_amount").alias("total_outstanding_loan"))

p = payment.groupBy("customer_id").agg(
    F.countDistinct("payment_id").alias("payment_count"),
    F.sum("payment_amount").alias("total_payment_amount"))

e = event.groupBy("customer_id").agg(
    F.countDistinct("event_id").alias("event_count"))

mart = (customer.select(
    "customer_key","customer_id","first_name","last_name","email","phone",
    "city","customer_segment","customer_status")
    .join(a,"customer_id","left").join(t,"customer_id","left")
    .join(l,"customer_id","left").join(p,"customer_id","left")
    .join(e,"customer_id","left").fillna(0))

TARGET_TABLE = "workspace.finlake_gold.customer_360"
mart.write.format("delta").mode("overwrite").option(
    "overwriteSchema","true").saveAsTable(TARGET_TABLE)

print(f"Created {TARGET_TABLE}")


# COMMAND ----------

print(spark.table(TARGET_TABLE).count())
assert spark.table(TARGET_TABLE).count() == 10500
