import random

import numpy as np
import pandas as pd
from faker import Faker

from config import (
    CUSTOMER_FILE,
    ACCOUNT_FILE,
    TRANSACTION_FILE,
    NUM_TRANSACTIONS,
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

TRANSACTION_TYPES = [
    "DEBIT",
    "CREDIT",
]

TRANSACTION_CATEGORIES = [
    "UPI",
    "ATM",
    "POS",
    "NEFT",
    "IMPS",
    "RTGS",
    "BANK_TRANSFER",
    "BILL_PAYMENT",
    "ECOMMERCE",
    "CASH_DEPOSIT",
    "CASH_WITHDRAWAL",
]

TRANSACTION_STATUSES = [
    "SUCCESS",
    "SUCCESS",
    "SUCCESS",
    "SUCCESS",
    "FAILED",
    "PENDING",
]

MERCHANTS = [
    "Amazon",
    "Flipkart",
    "Swiggy",
    "Zomato",
    "Uber",
    "BigBasket",
    "Myntra",
    "Reliance",
    "DMart",
    "Other",
]


# ============================================================
# GENERATOR
# ============================================================

def generate_transactions():

    customers = pd.read_csv(
        CUSTOMER_FILE
    )

    accounts = pd.read_csv(
        ACCOUNT_FILE
    )

    customer_ids = customers[
        "customer_id"
    ].tolist()

    account_customer_map = dict(
        zip(
            accounts["account_id"],
            accounts["customer_id"]
        )
    )

    account_ids = accounts[
        "account_id"
    ].tolist()

    records = []

    for i in range(1, NUM_TRANSACTIONS + 1):

        account_id = random.choice(
            account_ids
        )

        customer_id = account_customer_map[
            account_id
        ]

        transaction_type = random.choice(
            TRANSACTION_TYPES
        )

        records.append({
            "transaction_id": f"T{i:010d}",

            "account_id": account_id,

            "customer_id": customer_id,

            "transaction_type": transaction_type,

            "transaction_category": random.choice(
                TRANSACTION_CATEGORIES
            ),

            "amount": round(
                np.random.lognormal(
                    mean=7.5,
                    sigma=1.2
                ),
                2
            ),

            "transaction_date": fake.date_between(
                start_date="-2y",
                end_date="today"
            ),

            "transaction_timestamp": fake.date_time_between(
                start_date="-2y",
                end_date="now"
            ),

            "transaction_status": random.choice(
                TRANSACTION_STATUSES
            ),

            "merchant": random.choice(
                MERCHANTS
            ),

            "reference_number": fake.bothify(
                text="REF-########"
            ),
        })

    df = pd.DataFrame(records)

    df.to_csv(
        TRANSACTION_FILE,
        index=False
    )

    print(
        f"Generated {len(df):,} transactions"
    )

    print(
        f"Saved to: {TRANSACTION_FILE}"
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    generate_transactions()