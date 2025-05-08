# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: add schema and Read the data from pitstops json file 

# COMMAND ----------

dbutils.widgets.text("data_source", "")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import  StructField, StructType, StringType, IntegerType, DateType

# COMMAND ----------

pit_stop_schema = StructType( fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("stop", StringType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("duration", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)
                                      ])
                                   

# COMMAND ----------

pit_stop_df = spark.read \
.schema(pit_stop_schema) \
.option("multiline", "true")\
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Rename the required columns and add ingestion date 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------


pit_stop_final_df = pit_stop_df.withColumnRenamed("raceId", "race_id")\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

pit_stop_final_df = add_ingestion_date(pit_stop_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Write the dataframe to the datalake as parquet
# MAGIC

# COMMAND ----------

#overwrite_partition(pit_stop_final_df, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop"
merge_delta_table(pit_stop_final_df,"f1_processed", "pit_stops", "race_id",processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.pit_stops

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, COUNT(1) from f1_processed.pit_stops group by race_id order by race_id desc

# COMMAND ----------

dbutils.notebook.exit("Success")