import random

import numpy as np
import pandas as pd
from faker import Faker

from config import (
    CUSTOMER_FILE,
    NUM_CUSTOMERS,
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

GENDERS = [
    "MALE",
    "FEMALE",
    "OTHER",
]

CITIES = [
    "Bengaluru",
    "Mumbai",
    "Delhi",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
    "Bhubaneswar",
    "Ahmedabad",
    "Jaipur",
]

CUSTOMER_SEGMENTS = [
    "MASS",
    "PREMIUM",
    "WEALTH",
]

CUSTOMER_STATUSES = [
    "ACTIVE",
    "ACTIVE",
    "ACTIVE",
    "INACTIVE",
    "BLOCKED",
]

OCCUPATIONS = [
    "ENGINEER",
    "DOCTOR",
    "TEACHER",
    "BUSINESS_OWNER",
    "ACCOUNTANT",
    "LAWYER",
    "STUDENT",
    "CONSULTANT",
    "DESIGNER",
    "OTHER",
]


# ============================================================
# GENERATOR
# ============================================================

def generate_customers():

    records = []

    for i in range(1, NUM_CUSTOMERS + 1):

        records.append({
            "customer_id": f"C{i:07d}",

            "first_name": fake.first_name(),

            "last_name": fake.last_name(),

            "email": fake.email(),

            "phone": fake.phone_number(),

            "date_of_birth": fake.date_of_birth(
                minimum_age=18,
                maximum_age=75
            ),

            "gender": random.choice(GENDERS),

            "city": random.choice(CITIES),

            "occupation": random.choice(OCCUPATIONS),

            "customer_segment": random.choice(
                CUSTOMER_SEGMENTS
            ),

            "customer_status": random.choice(
                CUSTOMER_STATUSES
            ),

            "registration_date": fake.date_between(
                start_date="-10y",
                end_date="today"
            ),

            "created_at": fake.date_time_between(
                start_date="-10y",
                end_date="now"
            ),
        })

    df = pd.DataFrame(records)

    df.to_csv(
        CUSTOMER_FILE,
        index=False
    )

    print(f"Generated {len(df):,} customers")
    print(f"Saved to: {CUSTOMER_FILE}")


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    generate_customers()