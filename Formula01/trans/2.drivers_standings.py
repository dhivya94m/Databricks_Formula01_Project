# Databricks notebook source
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_col_to_list(race_results_df, 'race_year')


# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

race_results_grouped_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality")\
    .agg(sum("points").alias("total_points"), 
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_wins_window = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = race_results_grouped_df.withColumn("rank", rank().over(driver_wins_window))


# COMMAND ----------

#overwrite_partition(final_df, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_table(final_df,"f1_presentation", "driver_standings", "race_year",presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings