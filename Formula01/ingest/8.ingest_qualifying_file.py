# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: add schema and Read the data from Qualifying folder

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

qualifying_schema = StructType( fields = [StructField("qualifyId", IntegerType(), False),
                                        StructField("raceId", IntegerType(), True),
                                        StructField("driverId", IntegerType(), True),
                                        StructField("constructorId", IntegerType(), True),
                                        StructField("number", IntegerType(), True),
                                        StructField("position", IntegerType(), True),
                                        StructField("q1", StringType(), True),
                                        StructField("q2", StringType(), True),
                                        StructField("q3", StringType(), True)
                                      ])
                                   

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiline", "true")\
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Rename the required columns and add ingestion date 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------


qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
                                .withColumnRenamed("raceId", "race_id")\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("constructorId", "constructor_id")\
                                .withColumn("data_source", lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Write the dataframe to the datalake as parquet
# MAGIC

# COMMAND ----------

#overwrite_partition(qualifying_final_df, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_table(qualifying_final_df,"f1_processed", "qualifying", "race_id",processed_folder_path, merge_condition)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, COUNT(1) from f1_processed.qualifying group by race_id order by race_id desc
# MAGIC     

# COMMAND ----------

dbutils.notebook.exit("Success")