# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 6.4 — SCD Type 2 Idempotency Test
# MAGIC
# MAGIC This notebook verifies that processing the same CDC batch multiple times
# MAGIC does not create duplicate historical versions.
# MAGIC
# MAGIC Expected behavior:
# MAGIC
# MAGIC - Same CDC batch can be safely retried.
# MAGIC - Existing `(customer_id, effective_from)` versions are not duplicated.
# MAGIC - The SCD2 table remains unchanged after a rerun.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

SCD_TABLE = "workspace.finlake_silver.customer_scd2"
CDC_DATE = "2026-08-05"

# COMMAND ----------

before_df = spark.table(SCD_TABLE)

before_total = before_df.count()
before_unique = before_df.select("customer_id").distinct().count()
before_current = before_df.filter(F.col("is_current") == True).count()
before_expired = before_df.filter(F.col("is_current") == False).count()
before_cdc_versions = (
    before_df
    .filter(F.col("effective_from") == F.to_date(F.lit(CDC_DATE)))
    .count()
)

print(f"Total records       : {before_total:,}")
print(f"Unique customers    : {before_unique:,}")
print(f"Current records     : {before_current:,}")
print(f"Expired records     : {before_expired:,}")
print(f"CDC-date versions   : {before_cdc_versions:,}")

# COMMAND ----------

duplicate_versions = (
    before_df
    .groupBy("customer_id", "effective_from")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicate_versions.count()

print(f"Duplicate SCD versions: {duplicate_count}")

assert duplicate_count == 0

# COMMAND ----------

CDC_PATH = (
    "/Volumes/workspace/finlake_bronze/source_data/"
    "customers/customers_2026-08-05.csv"
)

tracked_columns = [
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "customer_segment",
    "customer_status"
]

incoming_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(CDC_PATH)
)

# COMMAND ----------

incoming_df = (
    incoming_df
    .withColumn("customer_id", F.trim(F.col("customer_id")))
    .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn(
        "phone",
        F.regexp_replace(F.col("phone"), r"[^0-9]", "")
    )
    .withColumn("city", F.initcap(F.trim(F.col("city"))))
    .withColumn(
        "customer_segment",
        F.upper(F.trim(F.col("customer_segment")))
    )
    .withColumn(
        "customer_status",
        F.upper(F.trim(F.col("customer_status")))
    )
)

# COMMAND ----------

current_df = (
    spark.table(SCD_TABLE)
    .filter(F.col("is_current") == True)
    .select("customer_id", *tracked_columns)
)

joined = (
    incoming_df
    .select("customer_id", *tracked_columns)
    .alias("incoming")
    .join(
        current_df.alias("current"),
        on="customer_id",
        how="left"
    )
)

# COMMAND ----------

change_condition = None

for column in tracked_columns:
    condition = ~(
        F.col(f"incoming.{column}")
        .eqNullSafe(F.col(f"current.{column}"))
    )

    change_condition = (
        condition
        if change_condition is None
        else change_condition | condition
    )

rerun_changes = (
    joined
    .withColumn(
        "change_type",
        F.when(
            F.col("current.customer_id").isNull(),
            "INSERT"
        )
        .when(change_condition, "UPDATE")
        .otherwise("UNCHANGED")
    )
)

# COMMAND ----------

display(
    rerun_changes
    .groupBy("change_type")
    .count()
    .orderBy("change_type")
)

# COMMAND ----------

after_df = spark.table(SCD_TABLE)

after_total = after_df.count()
after_unique = after_df.select("customer_id").distinct().count()
after_current = after_df.filter(F.col("is_current") == True).count()
after_expired = after_df.filter(F.col("is_current") == False).count()

after_cdc_versions = (
    after_df
    .filter(
        F.col("effective_from") ==
        F.to_date(F.lit(CDC_DATE))
    )
    .count()
)

print(f"Total records       : {after_total:,}")
print(f"Unique customers    : {after_unique:,}")
print(f"Current records     : {after_current:,}")
print(f"Expired records     : {after_expired:,}")
print(f"CDC-date versions   : {after_cdc_versions:,}")

# COMMAND ----------

assert after_total == before_total
assert after_unique == before_unique
assert after_current == before_current
assert after_expired == before_expired
assert after_cdc_versions == before_cdc_versions

print("======================================")
print("SCD2 IDEMPOTENCY TEST PASSED")
print("No duplicate historical versions created.")
print("======================================")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(*) AS versions
# MAGIC FROM workspace.finlake_silver.customer_scd2
# MAGIC GROUP BY customer_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY customer_id
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     city,
# MAGIC     customer_segment,
# MAGIC     customer_status,
# MAGIC     effective_from,
# MAGIC     effective_to,
# MAGIC     is_current
# MAGIC FROM workspace.finlake_silver.customer_scd2
# MAGIC WHERE customer_id = 'C0000005'
# MAGIC ORDER BY effective_from;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     customer_segment,
# MAGIC     customer_status,
# MAGIC     effective_from,
# MAGIC     effective_to
# MAGIC FROM workspace.finlake_silver.customer_scd2
# MAGIC WHERE customer_id = 'C0000231'
# MAGIC   AND DATE('2026-08-04') >= effective_from
# MAGIC   AND DATE('2026-08-04') < effective_to;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     city,
# MAGIC     customer_segment,
# MAGIC     customer_status,
# MAGIC     effective_from,
# MAGIC     effective_to
# MAGIC FROM workspace.finlake_silver.customer_scd2
# MAGIC WHERE customer_id = 'C0000231'
# MAGIC   AND DATE('2026-08-06') >= effective_from
# MAGIC   AND DATE('2026-08-06') < effective_to;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS invalid_records
# MAGIC FROM workspace.finlake_silver.customer_scd2
# MAGIC WHERE effective_from >= effective_to;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS customers_without_current_version
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         customer_id
# MAGIC     FROM workspace.finlake_silver.customer_scd2
# MAGIC     GROUP BY customer_id
# MAGIC     HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) = 0
# MAGIC );

# COMMAND ----------

