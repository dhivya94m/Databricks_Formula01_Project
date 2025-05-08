# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data lake Storage using the cluster scoped credentials
# MAGIC - 1. set the spark config in the cluster
# MAGIC - 2. list the files from demo folder
# MAGIC - 3. read the csv file from demo folder

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dmformula01.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dmformula01.dfs.core.windows.net/circuits.csv"))