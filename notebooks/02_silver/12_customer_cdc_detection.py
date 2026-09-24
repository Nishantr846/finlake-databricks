# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 6 — Customer CDC Detection
# MAGIC
# MAGIC This notebook compares the existing Silver customer state with an
# MAGIC incoming customer CDC batch.
# MAGIC
# MAGIC Each incoming record is classified as:
# MAGIC
# MAGIC - INSERT — customer_id does not exist in the current state
# MAGIC - UPDATE — customer_id exists but tracked attributes changed
# MAGIC - UNCHANGED — customer_id exists and no tracked attributes changed
# MAGIC
# MAGIC The resulting change classification will be used by the SCD Type 2 process.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

CURRENT_CUSTOMERS = "workspace.finlake_silver.customers"

current_df = spark.table(CURRENT_CUSTOMERS)

print(f"Current customer records: {current_df.count():,}")

display(current_df.limit(10))

# COMMAND ----------

incoming_path = "/Volumes/workspace/finlake_bronze/source_data/customers/customers_2026-08-05.csv"

incoming_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(incoming_path)
)

print(f"Incoming CDC records: {incoming_df.count():,}")

display(incoming_df.limit(10))

# COMMAND ----------

tracked_columns = [
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "customer_segment",
    "customer_status"
]

# COMMAND ----------

current_compare = current_df.select(
    "customer_id",
    *[
        F.col(c).alias(f"current_{c}")
        for c in tracked_columns
    ]
)

incoming_compare = incoming_df.select(
    "customer_id",
    *[
        F.col(c).alias(f"incoming_{c}")
        for c in tracked_columns
    ]
)

# COMMAND ----------

comparison_df = (
    incoming_compare.alias("incoming")
    .join(
        current_compare.alias("current"),
        on="customer_id",
        how="left"
    )
)

# COMMAND ----------

change_condition = None

for column in tracked_columns:

    condition = ~(
        F.col(f"incoming_{column}")
        .eqNullSafe(F.col(f"current_{column}"))
    )

    if change_condition is None:
        change_condition = condition
    else:
        change_condition = change_condition | condition

# COMMAND ----------

classified_df = (
    comparison_df
    .withColumn(
        "change_type",
        F.when(
            F.col("current_first_name").isNull(),
            F.lit("INSERT")
        )
        .when(
            change_condition,
            F.lit("UPDATE")
        )
        .otherwise(
            F.lit("UNCHANGED")
        )
    )
)

# COMMAND ----------

display(
    classified_df
    .groupBy("change_type")
    .count()
    .orderBy("change_type")
)

# COMMAND ----------

updates_df = (
    classified_df
    .filter(F.col("change_type") == "UPDATE")
)

display(updates_df.limit(20))

# COMMAND ----------

inserts_df = (
    classified_df
    .filter(F.col("change_type") == "INSERT")
)

display(inserts_df.limit(20))