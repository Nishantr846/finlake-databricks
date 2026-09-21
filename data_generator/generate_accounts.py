import random

import numpy as np
import pandas as pd
from faker import Faker

from config import (
    CUSTOMER_FILE,
    ACCOUNT_FILE,
    NUM_ACCOUNTS,
    RANDOM_SEED,
)


# ============================================================
# RANDOM SETUP
# ============================================================

fake = Faker("en_IN")

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)


# ============================================================
# CONSTANTS
# ============================================================

ACCOUNT_TYPES = [
    "SAVINGS",
    "CURRENT",
    "SALARY",
    "NRE",
    "NRO",
]

ACCOUNT_STATUSES = [
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "DORMANT",
    "CLOSED",
]

BRANCH_CODES = [
    "BLR001",
    "BLR002",
    "MUM001",
    "DEL001",
    "HYD001",
    "CHE001",
    "PUN001",
    "KOL001",
    "BBS001",
    "AMD001",
]


# ============================================================
# GENERATOR
# ============================================================

def generate_accounts():

    customers = pd.read_csv(
        CUSTOMER_FILE
    )

    customer_ids = customers[
        "customer_id"
    ].tolist()

    records = []

    for i in range(1, NUM_ACCOUNTS + 1):

        customer_id = random.choice(
            customer_ids
        )

        account_type = random.choice(
            ACCOUNT_TYPES
        )

        records.append({
            "account_id": f"A{i:08d}",

            "customer_id": customer_id,

            "account_number": fake.bban(),

            "account_type": account_type,

            "branch_code": random.choice(
                BRANCH_CODES
            ),

            "account_status": random.choice(
                ACCOUNT_STATUSES
            ),

            "opening_date": fake.date_between(
                start_date="-8y",
                end_date="today"
            ),

            "balance": round(
                np.random.uniform(
                    500,
                    5_000_000
                ),
                2
            ),

            "currency": "INR",

            "created_at": fake.date_time_between(
                start_date="-8y",
                end_date="now"
            ),
        })

    df = pd.DataFrame(records)

    df.to_csv(
        ACCOUNT_FILE,
        index=False
    )

    print(f"Generated {len(df):,} accounts")
    print(f"Saved to: {ACCOUNT_FILE}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    generate_accounts()