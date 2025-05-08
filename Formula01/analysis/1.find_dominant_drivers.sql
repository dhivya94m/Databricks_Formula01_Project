-- Databricks notebook source
show tables from f1_presentation

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT driver_name, sum(calculated_points) AS total_points
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
ORDER BY total_points DESC

-- COMMAND ----------

SELECT driver_name, 
COUNT(1) AS total_races,
sum(calculated_points) AS total_points,
AVG(calculated_points) AS avg_points
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2001 AND 2010
GROUP BY driver_name
HAVING total_races >= 50
ORDER BY avg_points DESC