-- Databricks notebook source
CREATE DATABASE demo

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo

-- COMMAND ----------

show databases

-- COMMAND ----------

desc database demo

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE demo;
SHOW TABLES

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CREATING AND WORKING WITH MANAGED TABLES

-- COMMAND ----------

-- MAGIC
-- MAGIC %run "../includes/configurations"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES FROM demo

-- COMMAND ----------

DESC EXTENDED demo.race_results_python

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_sql
SELECT * FROM demo.race_results_python WHERE race_year = 2020

-- COMMAND ----------

select * from demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES FROM demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE AND WORKING WITH EXTERNAL TABLES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").option("path", f"{presentation_folder_path}/race_results_ext_python").saveAsTable("demo.race_results_ext_python")

-- COMMAND ----------

show tables from demo

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_python

-- COMMAND ----------

select * from demo.race_results_ext_python

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql
(
  race_id INT,
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  race_time STRING,
  fastest_lap INT,
  points INT,
  position INT,
  created_date TIMESTAMP
)USING PARQUET
LOCATION 'abfss://presentation@dmformula01.dfs.core.windows.net/race_results_ext_sql'

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_python WHERE race_year = 2020

-- COMMAND ----------

SELECT count(*) FROM demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES FROM demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATING AND WORKING WITH VIEWS IN SQL

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results 
AS 
SELECT * FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results 
AS 
SELECT * FROM demo.race_results_python
WHERE race_year = 2019

-- COMMAND ----------

SHOW TABLES 

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results 
AS 
SELECT * FROM demo.race_results_python
WHERE race_year = 2000

-- COMMAND ----------

select * from demo.pv_race_results

-- COMMAND ----------

show tables in demo