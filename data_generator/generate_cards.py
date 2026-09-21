import random

import numpy as np
import pandas as pd
from faker import Faker

from config import (
    ACCOUNT_FILE,
    CARD_FILE,
    NUM_CARDS,
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

CARD_TYPES = [
    "DEBIT",
    "CREDIT",
]

CARD_NETWORKS = [
    "VISA",
    "MASTERCARD",
    "RUPAY",
]

CARD_STATUSES = [
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "BLOCKED",
    "EXPIRED",
]


# ============================================================
# GENERATOR
# ============================================================

def generate_cards():

    accounts = pd.read_csv(
        ACCOUNT_FILE
    )

    account_ids = accounts[
        "account_id"
    ].tolist()

    records = []
    
    for i in range(1, NUM_CARDS + 1):

        account_id = random.choice(
            account_ids
        )

        card_type = random.choice(
            CARD_TYPES
        )

        records.append({
            "card_id": f"CRD{i:08d}",

            "account_id": account_id,

            "card_type": card_type,

            "card_network": random.choice(
                CARD_NETWORKS
            ),

            "card_last_four": str(
                random.randint(
                    1000,
                    9999
                )
            ),

            "card_status": random.choice(
                CARD_STATUSES
            ),

            "issue_date": fake.date_between(
                start_date="-5y",
                end_date="today"
            ),

            "expiry_date": fake.date_between(
                start_date="today",
                end_date="+5y"
            ),

            "credit_limit": round(
                np.random.uniform(
                    25_000,
                    1_000_000
                ),
                2
            ) if card_type == "CREDIT" else 0,

            "created_at": fake.date_time_between(
                start_date="-5y",
                end_date="now"
            ),
        })
        
    df = pd.DataFrame(records)

    df.to_csv(
        CARD_FILE,
        index=False
    )

    print(f"Generated {len(df):,} cards")
    print(f"Saved to: {CARD_FILE}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    generate_cards()