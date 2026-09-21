import random

import numpy as np
import pandas as pd
from faker import Faker

from config import (
    CUSTOMER_FILE,
    LOAN_FILE,
    NUM_LOANS,
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

LOAN_TYPES = [
    "HOME",
    "PERSONAL",
    "AUTO",
    "EDUCATION",
    "BUSINESS",
]

LOAN_STATUSES = [
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "CLOSED",
    "DEFAULTED",
    "PENDING",
]

LOAN_PURPOSES = [
    "HOME_PURCHASE",
    "EDUCATION",
    "VEHICLE",
    "MEDICAL",
    "BUSINESS_EXPANSION",
    "PERSONAL",
    "OTHER",
]


# ============================================================
# GENERATOR
# ============================================================

def generate_loans():

    customers = pd.read_csv(
        CUSTOMER_FILE
    )

    customer_ids = customers[
        "customer_id"
    ].tolist()

    records = []

    for i in range(1, NUM_LOANS + 1):

        loan_type = random.choice(
            LOAN_TYPES
        )

        if loan_type == "HOME":
            principal = np.random.uniform(
                1_000_000,
                10_000_000
            )

            interest_rate = np.random.uniform(
                7.0,
                9.5
            )

        elif loan_type == "PERSONAL":
            principal = np.random.uniform(
                50_000,
                1_500_000
            )

            interest_rate = np.random.uniform(
                10.0,
                18.0
            )

        elif loan_type == "AUTO":
            principal = np.random.uniform(
                200_000,
                3_000_000
            )

            interest_rate = np.random.uniform(
                8.0,
                13.0
            )

        elif loan_type == "EDUCATION":
            principal = np.random.uniform(
                100_000,
                2_500_000
            )

            interest_rate = np.random.uniform(
                7.0,
                12.0
            )

        else:
            principal = np.random.uniform(
                500_000,
                10_000_000
            )

            interest_rate = np.random.uniform(
                8.0,
                15.0
            )

        tenure_months = random.choice([
            12,
            24,
            36,
            48,
            60,
            84,
            120,
            180,
            240,
        ])

        records.append({
            "loan_id": f"L{i:08d}",

            "customer_id": random.choice(
                customer_ids
            ),

            "loan_type": loan_type,

            "loan_purpose": random.choice(
                LOAN_PURPOSES
            ),

            "principal_amount": round(
                principal,
                2
            ),

            "interest_rate": round(
                interest_rate,
                2
            ),

            "tenure_months": tenure_months,

            "loan_status": random.choice(
                LOAN_STATUSES
            ),

            "disbursement_date": fake.date_between(
                start_date="-8y",
                end_date="today"
            ),

            "outstanding_amount": round(
                principal * np.random.uniform(
                    0.1,
                    0.95
                ),
                2
            ),

            "created_at": fake.date_time_between(
                start_date="-8y",
                end_date="now"
            ),
        })

    df = pd.DataFrame(records)

    df.to_csv(
        LOAN_FILE,
        index=False
    )

    print(f"Generated {len(df):,} loans")
    print(f"Saved to: {LOAN_FILE}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    generate_loans()