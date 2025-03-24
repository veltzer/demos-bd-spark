#!/usr/bin/env python

"""
Generate CSV
"""

import csv
import random
from faker import Faker


def generate_csv(filename, num_rows):
    fake = Faker()
    departments = ["Engineering", "Marketing", "HR", "Sales", "Finance"]

    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["id", "name", "age", "department", "salary"])

        for i in range(num_rows):
            writer.writerow([
                i + 1,
                fake.name(),
                random.randint(22, 60),
                random.choice(departments),
                random.randint(30000, 150000)
            ])

if __name__ == "__main__":
    generate_csv("large_input.csv", 10000)
