-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed;
SHOW TABLES

-- COMMAND ----------

SELECT * FROM f1_processed.drivers
WHERE dob >= '1990-01-01'
AND nationality = 'French'
ORDER BY dob DESC

-- COMMAND ----------

SELECT * FROM f1_processed.drivers
order by nationality ASC,
dob DESC
