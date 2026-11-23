#!/usr/bin/env python

"""
Solution
"""

with open("passwd") as stream:
    # if I break here, the "passwd" file has already been opened
    counter = 0
    for line in stream:
        if line.rstrip().endswith(":/bin/bash"):
            counter = counter + 1
print(f"counter is {counter}...")
