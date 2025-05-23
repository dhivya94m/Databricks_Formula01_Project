# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: add schema and Read the data from results json file 

# COMMAND ----------

dbutils.widgets.text("data_source", "")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import  StructField, StructType, StringType, IntegerType, DateType

# COMMAND ----------

results_schema = StructType( fields = [StructField("resultId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("grid", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("positionText", StringType(), True),
                                       StructField("positionOrder", IntegerType(), True),
                                       StructField("points", IntegerType(), True),
                                       StructField("laps", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True),
                                       StructField("fastestLap", IntegerType(), True),
                                       StructField("rank", IntegerType(), True),
                                       StructField("fastestLapTime", StringType(), True),
                                       StructField("fastestLapSpeed", StringType(), True),
                                       StructField("statusId", StringType(), True),
                                       
                                      ])
                                   

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Rename the required columns and add ingestion date 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------


results_renamed_df = results_df.withColumnRenamed("resultId", "result_id") \
                                .withColumnRenamed("raceId", "race_id")\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("constructorId", "constructor_id")\
                                .withColumnRenamed("positionText", "position_text")\
                                .withColumnRenamed("positionOrder", "position_order")\
                                .withColumnRenamed("fastestLap", "fastest_lap")\
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_renamed_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Drop the unnecessary columns

# COMMAND ----------

results_final_df = results_renamed_df.drop(col("statusId"))

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Write the dataframe to the datalake as parquet
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write \
# .mode("append") \
# .partitionBy("race_id") \
# .format("parquet") \
# .saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC   --DROP TABLE IF EXISTS f1_processed.results

# COMMAND ----------

#overwrite_partition(results_final_df, "f1_processed", "results", "race_id")

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_table(results_deduped_df,"f1_processed", "results", "race_id",processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, COUNT(1) from f1_processed.results group by race_id order by race_id desc
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, driver_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id, driver_id
# MAGIC HAVING COUNT(1) > 1
# MAGIC ORDER BY race_id, driver_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")