# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 6 — Customer SCD Type 2
# MAGIC
# MAGIC This notebook bootstraps and maintains a Slowly Changing Dimension Type 2 table.
# MAGIC
# MAGIC - Clean 2026-08-03 snapshot = initial baseline
# MAGIC - 2026-08-05 CDC batch = 500 inserts + 500 actual updates + 9,500 unchanged
# MAGIC - Null-safe column comparison detects changes
# MAGIC - Updated versions are expired
# MAGIC - New current versions are inserted
# MAGIC - Same CDC batch is protected against duplicate historical versions
# MAGIC
# MAGIC The 2026-08-04 batch is intentionally excluded because it was corrupted for Phase 5 DQ testing.
# MAGIC

# COMMAND ----------

SCD_TABLE = "workspace.finlake_silver.customer_scd2"
# spark.sql(f"DROP TABLE IF EXISTS {SCD_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

SCD_TABLE = "workspace.finlake_silver.customer_scd2"

BASELINE_PATH = (
    "/Volumes/workspace/finlake_bronze/source_data/"
    "customers/customers_2026-08-03.csv"
)

CDC_PATH = (
    "/Volumes/workspace/finlake_bronze/source_data/"
    "customers/customers_2026-08-05.csv"
)

BASELINE_DATE = "2026-08-03"
CDC_DATE = "2026-08-05"
OPEN_END_DATE = "9999-12-31"

tracked_columns = [
    "first_name",
    "last_name",
    "email",
    "phone",
    "city",
    "customer_segment",
    "customer_status"
]

print("SCD table:", SCD_TABLE)
print("Baseline:", BASELINE_PATH)
print("CDC batch:", CDC_PATH)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reset the previous incorrect SCD2 table
# MAGIC
# MAGIC Run this notebook from the beginning once to rebuild the table cleanly.
# MAGIC This cell intentionally drops the previous incorrect SCD2 result.
# MAGIC
# MAGIC **Do not rerun the notebook from the beginning after the first successful run.**
# MAGIC

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SCD_TABLE}")
print(f"Reset complete: {SCD_TABLE} dropped.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Common customer standardization
# MAGIC
# MAGIC The same normalization is applied to the baseline and CDC data.
# MAGIC

# COMMAND ----------

def standardize_customers(df):
    return (
        df
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("phone", F.regexp_replace(F.col("phone"), r"[^0-9]", ""))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("customer_segment", F.upper(F.trim(F.col("customer_segment"))))
        .withColumn("customer_status", F.upper(F.trim(F.col("customer_status"))))
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load clean 2026-08-03 baseline

# COMMAND ----------

baseline_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(BASELINE_PATH)
)

baseline_df = standardize_customers(baseline_df)

baseline_count = baseline_df.count()
baseline_unique = baseline_df.select("customer_id").distinct().count()

print(f"Baseline records: {baseline_count:,}")
print(f"Unique customer IDs: {baseline_unique:,}")

assert baseline_count == 10000
assert baseline_unique == 10000


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Load 2026-08-05 CDC batch

# COMMAND ----------

incoming_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .load(CDC_PATH)
)

incoming_df = standardize_customers(incoming_df)

incoming_count = incoming_df.count()
incoming_unique = incoming_df.select("customer_id").distinct().count()

print(f"Incoming records: {incoming_count:,}")
print(f"Unique customer IDs: {incoming_unique:,}")

assert incoming_count == 10500
assert incoming_unique == 10500


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create initial SCD2 snapshot

# COMMAND ----------

initial_scd_df = (
    baseline_df
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "customer_segment",
        "customer_status"
    )
    .withColumn("effective_from", F.to_date(F.lit(BASELINE_DATE)))
    .withColumn("effective_to", F.to_date(F.lit(OPEN_END_DATE)))
    .withColumn("is_current", F.lit(True))
    .withColumn("scd_created_at", F.current_timestamp())
)

(
    initial_scd_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SCD_TABLE)
)

print("Initial SCD2 snapshot created.")


# COMMAND ----------

initial = spark.table(SCD_TABLE)

print(f"Initial total records: {initial.count():,}")
print(f"Initial unique customers: {initial.select('customer_id').distinct().count():,}")
print(f"Initial current records: {initial.filter(F.col('is_current') == True).count():,}")

assert initial.count() == 10000


# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Detect INSERT / UPDATE / UNCHANGED
# MAGIC
# MAGIC Only the current SCD version is compared with the incoming CDC record.
# MAGIC `eqNullSafe` makes NULL-to-NULL comparisons behave correctly.
# MAGIC

# COMMAND ----------

current_df = (
    spark.table(SCD_TABLE)
    .filter(F.col("is_current") == True)
    .select("customer_id", *tracked_columns)
)

incoming_compare = incoming_df.select("customer_id", *tracked_columns)
current_compare = current_df.select("customer_id", *tracked_columns)

joined = (
    incoming_compare.alias("incoming")
    .join(
        current_compare.alias("current"),
        on="customer_id",
        how="left"
    )
)

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

classified_df = (
    joined
    .withColumn(
        "change_type",
        F.when(
            F.col("current.customer_id").isNull(),
            F.lit("INSERT")
        )
        .when(change_condition, F.lit("UPDATE"))
        .otherwise(F.lit("UNCHANGED"))
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. CDC classification

# COMMAND ----------

display(
    classified_df
    .groupBy("change_type")
    .count()
    .orderBy("change_type")
)


# COMMAND ----------

# Diagnose which tracked columns are producing differences

diagnostic_conditions = []

for column in tracked_columns:
    mismatch_count = (
        classified_df
        .filter(
            ~(
                F.col(f"incoming.{column}")
                .eqNullSafe(F.col(f"current.{column}"))
            )
        )
        .count()
    )

    diagnostic_conditions.append(
        (column, mismatch_count)
    )

diagnostic_df = spark.createDataFrame(
    diagnostic_conditions,
    ["column_name", "mismatch_count"]
)

display(
    diagnostic_df
    .orderBy(F.desc("mismatch_count"))
)

# COMMAND ----------

existing_only = classified_df.filter(
    F.col("current.customer_id").isNotNull()
)

diagnostic_existing = []

for column in tracked_columns:
    mismatch_count = (
        existing_only
        .filter(
            ~(
                F.col(f"incoming.{column}")
                .eqNullSafe(F.col(f"current.{column}"))
            )
        )
        .count()
    )

    diagnostic_existing.append(
        (column, mismatch_count)
    )

diagnostic_existing_df = spark.createDataFrame(
    diagnostic_existing,
    ["column_name", "mismatch_count"]
)

display(
    diagnostic_existing_df
    .orderBy(F.desc("mismatch_count"))
)

# COMMAND ----------

display(
    existing_only
    .select(
        "customer_id",
        F.col("current.phone").alias("old_phone"),
        F.col("incoming.phone").alias("new_phone")
    )
    .filter(
        ~F.col("incoming.phone").eqNullSafe(
            F.col("current.phone")
        )
    )
    .limit(20)
)

# COMMAND ----------

display(
    existing_only
    .select(
        "customer_id",
        F.col("current.city").alias("old_city"),
        F.col("incoming.city").alias("new_city")
    )
    .filter(
        ~F.col("incoming.city").eqNullSafe(
            F.col("current.city")
        )
    )
    .limit(20)
)

# COMMAND ----------

classification = {
    row["change_type"]: row["count"]
    for row in classified_df.groupBy("change_type").count().collect()
}

print("INSERT    :", classification.get("INSERT", 0))
print("UPDATE    :", classification.get("UPDATE", 0))
print("UNCHANGED :", classification.get("UNCHANGED", 0))

assert classification.get("INSERT", 0) == 500
assert classification.get("UPDATE", 0) == 500
assert classification.get("UNCHANGED", 0) == 9500

print("CDC classification PASSED.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Prepare versions for INSERT and UPDATE

# COMMAND ----------

new_versions = (
    classified_df
    .filter(F.col("change_type").isin("INSERT", "UPDATE"))
    .select(
        "customer_id",
        *[
            F.col(f"incoming.{column}").alias(column)
            for column in tracked_columns
        ]
    )
    .withColumn("effective_from", F.to_date(F.lit(CDC_DATE)))
    .withColumn("effective_to", F.to_date(F.lit(OPEN_END_DATE)))
    .withColumn("is_current", F.lit(True))
    .withColumn("scd_created_at", F.current_timestamp())
)

print(f"Candidate new versions: {new_versions.count():,}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Expire previous versions for UPDATE records

# COMMAND ----------

update_keys = (
    classified_df
    .filter(F.col("change_type") == "UPDATE")
    .select("customer_id")
    .distinct()
)

update_count = update_keys.count()
print(f"Versions to expire: {update_count:,}")

if update_count > 0:
    delta_scd = DeltaTable.forName(spark, SCD_TABLE)

    (
        delta_scd.alias("target")
        .merge(
            update_keys.alias("source"),
            "target.customer_id = source.customer_id AND target.is_current = true"
        )
        .whenMatchedUpdate(
            set={
                "effective_to": F.to_date(F.lit(CDC_DATE)),
                "is_current": F.lit(False)
            }
        )
        .execute()
    )

    print("Previous versions expired.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Insert new current versions idempotently
# MAGIC
# MAGIC A version is unique by `(customer_id, effective_from)`. If it already exists,
# MAGIC the anti-join prevents a duplicate historical version.
# MAGIC

# COMMAND ----------

existing_versions = (
    spark.table(SCD_TABLE)
    .select("customer_id", "effective_from")
    .dropDuplicates()
)

versions_to_insert = (
    new_versions.alias("new")
    .join(
        existing_versions.alias("existing"),
        (
            (F.col("new.customer_id") == F.col("existing.customer_id")) &
            (F.col("new.effective_from") == F.col("existing.effective_from"))
        ),
        "left_anti"
    )
)

insert_count = versions_to_insert.count()

print(f"Versions to insert: {insert_count:,}")

if insert_count > 0:
    (
        versions_to_insert
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(SCD_TABLE)
    )

    print("New versions inserted.")
else:
    print("No new versions to insert.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Final SCD2 validation

# COMMAND ----------

final_scd_df = spark.table(SCD_TABLE)

total_records = final_scd_df.count()
unique_customers = final_scd_df.select("customer_id").distinct().count()
current_records = final_scd_df.filter(F.col("is_current") == True).count()
expired_records = final_scd_df.filter(F.col("is_current") == False).count()

print(f"Total historical records : {total_records:,}")
print(f"Unique customers         : {unique_customers:,}")
print(f"Current records          : {current_records:,}")
print(f"Expired records          : {expired_records:,}")

assert total_records == 11000, f"Expected 11000, found {total_records}"
assert unique_customers == 10500, f"Expected 10500, found {unique_customers}"
assert current_records == 10500, f"Expected 10500, found {current_records}"
assert expired_records == 500, f"Expected 500, found {expired_records}"

print("SCD2 validation PASSED.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Inspect historical versions

# COMMAND ----------

display(
    final_scd_df
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
    .limit(20)
)


# COMMAND ----------

history_ids = [
    row["customer_id"]
    for row in (
        final_scd_df
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") == 2)
        .limit(5)
        .collect()
    )
]

display(
    final_scd_df
    .filter(F.col("customer_id").isin(history_ids))
    .select(
        "customer_id",
        "city",
        "customer_segment",
        "effective_from",
        "effective_to",
        "is_current"
    )
    .orderBy("customer_id", "effective_from")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Idempotency baseline
# MAGIC
# MAGIC The first run should create exactly 989 records with `effective_from = 2026-08-05`:
# MAGIC 489 updated versions + 500 inserted customers.
# MAGIC
# MAGIC A later rerun of the same CDC batch must insert zero additional versions.
# MAGIC

# COMMAND ----------

cdc_date_versions = (
    spark.table(SCD_TABLE)
    .filter(
        F.col("effective_from") == F.to_date(F.lit(CDC_DATE))
    )
    .count()
)

print(f"Versions for {CDC_DATE}: {cdc_date_versions:,}")

assert cdc_date_versions == 1000
print("Idempotency baseline PASSED.")


# COMMAND ----------

# MAGIC %md
# MAGIC               10,500 CDC records
# MAGIC                      │
# MAGIC           ┌──────────┼──────────┐
# MAGIC           ▼          ▼          ▼
# MAGIC        INSERT      UPDATE    UNCHANGED
# MAGIC          500         500        9500
# MAGIC           │           │
# MAGIC           │           ▼
# MAGIC           │       Expire old
# MAGIC           │       versions
# MAGIC           │           │
# MAGIC           └─────┬─────┘
# MAGIC                 ▼
# MAGIC           Insert 1,000
# MAGIC           new versions
# MAGIC                 │
# MAGIC                 ▼
# MAGIC         SCD2 = 11,000 rows