# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

circuits_df = circuits_df.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")


# COMMAND ----------

races_df = races_df.withColumnRenamed("name", "race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Inner Join

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")\
    .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_id, races_df.race_name)

# COMMAND ----------

display(circuits_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Outer join
# MAGIC  

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .filter("circuit_id < 70")\
        .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------


races_df = spark.read.parquet(f"{processed_folder_path}/races")\
.filter("race_year == 2019")\
.withColumnRenamed("name", "race_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### left outer join

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left")\
    .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_id, races_df.race_name)

# COMMAND ----------

display(circuits_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### right outer join

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right")\
    .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_id, races_df.race_name)

# COMMAND ----------

display(circuits_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### full outer join

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full")\
    .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_id, races_df.race_name)

# COMMAND ----------

display(circuits_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### semi join

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(circuits_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### anti join

# COMMAND ----------

circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(circuits_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### cross join

# COMMAND ----------

circuits_races_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

display(circuits_races_df)