# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: add schema and Read the data from drivers json file 

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

name_schema = StructType( fields = [StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True)
                                   ])

# COMMAND ----------

drivers_schema = StructType( fields = [StructField("driverId", IntegerType(), False),
                                       StructField("driverRef", StringType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("code", StringType(), True),
                                       StructField("name", name_schema),
                                       StructField("url", StringType(), True),
                                       StructField("nationality", StringType(), True),
                                       StructField("dob", DateType(), True),
                                      ])
                                   

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Rename the required columns and add ingestion date and combine the name column data
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, current_timestamp

# COMMAND ----------


drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_renamed_df = add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Drop the unnecessary columns

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Write the dataframe to the datalake as parquet
# MAGIC

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")