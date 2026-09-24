import pandas as pd
import numpy as np

from config import CUSTOMERS_PATH


# ============================================================
# Configuration
# ============================================================

SOURCE_BATCH = "2026-08-03"
CDC_BATCH = "2026-08-05"

UPDATE_COUNT = 500
INSERT_COUNT = 500

RANDOM_SEED = 100

rng = np.random.default_rng(RANDOM_SEED)


# ============================================================
# Paths
# ============================================================

source_file = (
    CUSTOMERS_PATH /
    f"customers_{SOURCE_BATCH}.csv"
)

output_file = (
    CUSTOMERS_PATH /
    f"customers_{CDC_BATCH}.csv"
)


# ============================================================
# Load previous customer state
# ============================================================

customers = pd.read_csv(
    source_file,
    dtype={
        "customer_id": str,
        "first_name": str,
        "last_name": str,
        "email": str,
        "phone": str,
        "city": str,
        "customer_segment": str,
        "customer_status": str
    }
)

print(f"Source customers: {len(customers):,}")


# ============================================================
# 1. UPDATE EXISTING CUSTOMERS
# ============================================================

update_indices = rng.choice(
    customers.index,
    size=UPDATE_COUNT,
    replace=False
)

cities = [
    "Bangalore",
    "Hyderabad",
    "Mumbai",
    "Delhi",
    "Pune",
    "Chennai"
]

# Guarantee that every selected customer gets a different city
for idx in update_indices:
    current_city = customers.loc[idx, "city"]

    valid_cities = [
        city for city in cities
        if city != current_city
    ]

    customers.loc[idx, "city"] = rng.choice(valid_cities)


# Also update customer segment for some customers

segments = [
    "STANDARD",
    "PREMIUM",
    "VIP"
]

segment_indices = update_indices[:100]

customers.loc[
    segment_indices,
    "customer_segment"
] = rng.choice(
    segments,
    size=len(segment_indices)
)


# ============================================================
# 2. CREATE NEW CUSTOMERS
# ============================================================

new_customers = customers.iloc[:INSERT_COUNT].copy()

new_customer_ids = [
    f"CUST_NEW_{i:06d}"
    for i in range(1, INSERT_COUNT + 1)
]

new_customers["customer_id"] = new_customer_ids

new_customers["first_name"] = [
    f"NewCustomer{i}"
    for i in range(1, INSERT_COUNT + 1)
]

new_customers["last_name"] = [
    "FinLake"
    for _ in range(INSERT_COUNT)
]

new_customers["email"] = [
    f"newcustomer{i}@finlake.com"
    for i in range(1, INSERT_COUNT + 1)
]

new_customers["city"] = rng.choice(
    cities,
    size=INSERT_COUNT
)

new_customers["customer_segment"] = rng.choice(
    segments,
    size=INSERT_COUNT
)

new_customers["_batch_date"] = CDC_BATCH


# ============================================================
# 3. Combine UPDATE + INSERT
# ============================================================

cdc_batch = pd.concat(
    [
        customers,
        new_customers
    ],
    ignore_index=True
)


# ============================================================
# 4. Save CDC batch
# ============================================================

cdc_batch.to_csv(
    output_file,
    index=False
)


# ============================================================
# Summary
# ============================================================

print("\n==========================================")
print("Customer CDC batch generated")
print("==========================================")

print(f"Source batch     : {SOURCE_BATCH}")
print(f"CDC batch        : {CDC_BATCH}")
print(f"Existing records : {len(customers):,}")
print(f"Updated records  : {UPDATE_COUNT:,}")
print(f"New records      : {INSERT_COUNT:,}")
print(f"Total CDC rows   : {len(cdc_batch):,}")
print(f"Output           : {output_file}")