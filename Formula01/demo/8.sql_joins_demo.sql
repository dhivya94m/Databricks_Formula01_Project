-- Databricks notebook source
SHOW TABLES FROM f1_presentation

-- COMMAND ----------

DESC f1_presentation.driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_driver_standings_2018
AS
SELECT race_year, driver_name,  total_points, driver_nationality, rank
FROM f1_presentation.driver_standings
WHERE race_year = 2018
ORDER BY rank

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW vw_driver_standings_2020
AS
SELECT race_year, driver_name,  total_points, driver_nationality, rank
FROM f1_presentation.driver_standings
WHERE race_year = 2020
ORDER BY rank

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inner Join

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2018 AS d1_2018
JOIN vw_driver_standings_2020 AS d2_2020
ON d1_2018.driver_name = d2_2020.driver_name


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Left Outer Join

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2018 AS d1_2018
LEFT JOIN vw_driver_standings_2020 AS d2_2020
ON d1_2018.driver_name = d2_2020.driver_name

-- COMMAND ----------

#### Right Outer join

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2018 AS d1_2018
RIGHT JOIN vw_driver_standings_2020 AS d2_2020
ON d1_2018.driver_name = d2_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### FULL JOIN

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2018 AS d1_2018
FULL JOIN vw_driver_standings_2020 AS d2_2020
ON d1_2018.driver_name = d2_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### SEMI JOIN

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2018 AS d1_2018
SEMI JOIN vw_driver_standings_2020 AS d2_2020
ON d1_2018.driver_name = d2_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### ANTI JOIN

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2018 AS d1_2018
ANTI JOIN vw_driver_standings_2020 AS d2_2020
ON d1_2018.driver_name = d2_2020.driver_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CROSS JOIN

-- COMMAND ----------

SELECT * FROM vw_driver_standings_2018 AS d1_2018
CROSS JOIN vw_driver_standings_2020 AS d2_2020