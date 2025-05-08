-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION 'abfss://processed@dmformula01.dfs.core.windows.net/'

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION 'abfss://presentation@dmformula01.dfs.core.windows.net/'