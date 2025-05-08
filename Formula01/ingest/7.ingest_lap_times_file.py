# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("data_source", "")
v_data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: add schema and Read the data from lap_times folder

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import  StructField, StructType, StringType, IntegerType, DateType

# COMMAND ----------

lap_times_schema = StructType( fields = [StructField("raceId", IntegerType(), False),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("lap", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("milliseconds", IntegerType(), True)
                                      ])
                                   

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Rename the required columns and add ingestion date 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------


lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id")\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumn("dataSource", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

lap_times_final_df = add_ingestion_date(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Write the dataframe to the datalake as parquet
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC  --DROP TABLE IF EXISTS f1_processed.lap_times

# COMMAND ----------

#overwrite_partition(lap_times_final_df, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"

merge_delta_table(lap_times_final_df,"f1_processed", "lap_times", "race_id",processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, COUNT(1) from f1_processed.lap_times group by race_id order by race_id desc
# MAGIC     

# COMMAND ----------

dbutils.notebook.exit("Success")