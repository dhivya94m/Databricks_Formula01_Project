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

print(race_year_list)

# COMMAND ----------


race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results")\
    .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

race_results_grouped_df = race_results_df.groupBy("race_year", "team")\
    .agg(sum("points").alias("total_points"), 
         count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

display(race_results_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructors_wins_window = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = race_results_grouped_df.withColumn("rank", rank().over(constructors_wins_window))


# COMMAND ----------

display(final_df.filter("race_year in (2018,2019,2020) " ))

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_table(final_df,"f1_presentation", "constructor_standings", "race_year",presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings