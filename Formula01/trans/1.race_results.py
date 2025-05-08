# Databricks notebook source
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("name", "circuit_name")\
    .withColumnRenamed("location", "circuit_location")

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name", "team")


# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races")\
    .withColumnRenamed("name", "race_name")\
    .withColumnRenamed("race_timestamp", "race_date")


# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results")\
    .filter(f"file_date = '{v_file_date}'")\
    .withColumnRenamed("time", "race_time")\
    .withColumnRenamed("race_id", "result_race_id")\
    .withColumnRenamed("file_date", "results_file_date")

# COMMAND ----------


races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")\
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id)\
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)\
        .select(races_circuits_df.race_id, races_circuits_df.race_year, races_circuits_df.race_name, races_circuits_df.race_date, races_circuits_df.circuit_location, drivers_df.driver_name, drivers_df.driver_number,drivers_df.driver_nationality, constructors_df.team, results_df.grid, results_df.race_time, results_df.fastest_lap, results_df.points, results_df.position, results_df.results_file_date)\
    .withColumn("created_date", current_timestamp())\
    .withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

final_df = race_results_df.orderBy(race_results_df.points.desc())

# COMMAND ----------

#overwrite_partition(final_df, "f1_presentation", "race_results", "race_id")


# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_table(final_df,"f1_presentation", "race_results", "race_id",presentation_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, COUNT(1) from f1_presentation.race_results group by race_id order by race_id desc