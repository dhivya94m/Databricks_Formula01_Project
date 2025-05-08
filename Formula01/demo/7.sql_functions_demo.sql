-- Databricks notebook source
SELECT *, concat(driver_ref, '-', code) AS driver_code FROM f1_processed.drivers

-- COMMAND ----------

SELECT *, split(name, " ")[0] AS first_name, split(name, " ")[1] AS last_name  FROM f1_processed.drivers

-- COMMAND ----------

SELECT *, current_timestamp()   FROM f1_processed.drivers

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy') AS formatted_dob  FROM f1_processed.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Aggregate functions
-- MAGIC

-- COMMAND ----------

SELECT COUNT(*) FROM f1_processed.drivers

-- COMMAND ----------

SELECT nationality, COUNT(*) AS drivers_count
FROM f1_processed.drivers
GROUP BY nationality
HAVING drivers_count > 50
ORDER BY drivers_count DESC


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Window functions

-- COMMAND ----------

SELECT  name, nationality, dob, rank() OVER(PARTITION BY nationality ORDER BY dob DESC) as age_rank 
FROM f1_processed.drivers
ORDER BY  nationality ASC,age_rank ASC

-- COMMAND ----------

S