# SQL schema inference

You are given a standard UNIX /etc/passwd file.

Write a pyspark script that shows which users have `/bin/bash` as their shell.

Since this is a text file we will need to use schema inference in order use
pyspark SQL with this file.

In plain words this means that we will need to describe the structure of this file
as a table to pyspark.sql so that we will be able to do sql operations on it.
