#!/usr/bin/python

with open("passwd", "r") as stream:
    counter = 0
    for line in stream:
        line = line.rstrip()
        if line.endswith(":/bin/bash"):
            counter = counter + 1
print(f"counter is {counter}...")
