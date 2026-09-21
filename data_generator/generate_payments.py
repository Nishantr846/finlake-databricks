import random

import numpy as np
import pandas as pd
from faker import Faker

from config import (
    LOAN_FILE,
    PAYMENT_FILE,
    NUM_PAYMENTS,
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

PAYMENT_STATUSES = [
    "SUCCESS",
    "SUCCESS",
    "SUCCESS",
    "PENDING",
    "FAILED",
    "OVERDUE",
]

PAYMENT_METHODS = [
    "UPI",
    "NET_BANKING",
    "DEBIT_CARD",
    "CREDIT_CARD",
    "BANK_TRANSFER",
    "AUTO_DEBIT",
]


# ============================================================
# GENERATOR
# ============================================================

def generate_payments():

    loans = pd.read_csv(
        LOAN_FILE
    )

    loan_customer = dict(
        zip(
            loans["loan_id"],
            loans["customer_id"]
        )
    )

    loan_ids = loans[
        "loan_id"
    ].tolist()

    records = []

    for i in range(1, NUM_PAYMENTS + 1):

        loan_id = random.choice(
            loan_ids
        )

        customer_id = loan_customer[
            loan_id
        ]

        records.append({
            "payment_id": f"P{i:08d}",

            "loan_id": loan_id,

            "customer_id": customer_id,

            "payment_amount": round(
                np.random.uniform(
                    1_000,
                    100_000
                ),
                2
            ),

            "payment_date": fake.date_between(
                start_date="-2y",
                end_date="today"
            ),

            "payment_status": random.choice(
                PAYMENT_STATUSES
            ),

            "payment_method": random.choice(
                PAYMENT_METHODS
            ),
        })

    df = pd.DataFrame(records)

    df.to_csv(
        PAYMENT_FILE,
        index=False
    )

    print(f"Generated {len(df):,} payments")
    print(f"Saved to: {PAYMENT_FILE}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    generate_payments()