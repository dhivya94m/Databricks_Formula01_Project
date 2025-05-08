# Databricks notebook source
# MAGIC %md
# MAGIC # Explore dbutils security
# MAGIC

# COMMAND ----------

dbutils.secrets.help()


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula01-scope")

# COMMAND ----------

dbutils.secrets.get(scope="formula01-scope", key="formula01dl-sas-key")