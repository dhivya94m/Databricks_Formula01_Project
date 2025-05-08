# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct, sum

# COMMAND ----------

race_aggr_df = race_results_df.select(count("*")).show()

# COMMAND ----------

race_aggr_df = race_results_df.filter("race_year = 2020").select(count("*")).show()

# COMMAND ----------

race_aggr_df = race_results_df.filter("race_year = 2020").select(countDistinct("race_id")).show()

# COMMAND ----------

race_aggr_df = race_results_df.filter("race_year = 2020 and driver_name = 'Lewis Hamilton'")\
    .select(sum("points"), countDistinct("race_id"))\
    .withColumnRenamed("sum(points)", "total_points")\
    .withColumnRenamed("count(DISTINCT race_id)", "number_of_races")\
    .show()
    

# COMMAND ----------

# MAGIC %md
# MAGIC #### groupBy aggregation

# COMMAND ----------

from pyspark.sql.functions import desc

# COMMAND ----------

demo_df = race_results_df.filter("race_year = 2020")\
    .groupBy("driver_name")\
    .agg(sum("points").alias("total_points"), countDistinct("race_id").alias("number_of_races"))
display(demo_df.orderBy(desc("total_points")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### windows functions

# COMMAND ----------

demo_df = race_results_df.filter("race_year in (2019, 2020)")\
    .groupBy(race_results_df.driver_name, race_results_df.race_year)\
    .agg(sum("points").alias("total_points"), countDistinct("race_id").alias("number_of_races"))
display(demo_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

windowSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))
window_df = demo_df.withColumn("rank", rank().over(windowSpec))
display(window_df)