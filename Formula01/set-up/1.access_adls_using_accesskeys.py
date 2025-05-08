# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data lake Storage using the access keys
# MAGIC - 1. set the spark config
# MAGIC - 2. list the files from demo folder
# MAGIC - 3. read the csv file from demo folder

# COMMAND ----------

formula01_accountkey = dbutils.secrets.get(scope="formula01-scope", key="formula1-dl-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.dmformula01.dfs.core.windows.net",
    formula01_accountkey
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dmformula01.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dmformula01.dfs.core.windows.net/circuits.csv"))