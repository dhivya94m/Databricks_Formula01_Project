# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data lake Storage using the sas token
# MAGIC - 1. set the spark config
# MAGIC - 2. list the files from demo folder
# MAGIC - 3. read the csv file from demo folder

# COMMAND ----------

formula1_sas_token = dbutils.secrets.get(scope="formula01-scope", key="formula01dl-sas-key")

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.dmformula01.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dmformula01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dmformula01.dfs.core.windows.net", formula1_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dmformula01.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@dmformula01.dfs.core.windows.net/circuits.csv"))