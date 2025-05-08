# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_year = 2018

# COMMAND ----------

race_results_by_year = spark.sql(f"select * from v_race_results where race_year = {race_year}")
display(race_results_by_year)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global temp views
# MAGIC - can be accessed outside of the notebook session.. ie in other notebooks

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results
# MAGIC where race_year = 2020