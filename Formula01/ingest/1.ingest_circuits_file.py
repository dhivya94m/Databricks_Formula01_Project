# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Read the data and add schema

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields= [StructField("circuitId", IntegerType(), False),
                                        StructField("circuitRef", StringType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("location", StringType(), True),
                                        StructField("country", StringType(), True),
                                        StructField("lat", DoubleType(), True),
                                        StructField("lng", DoubleType(), True),
                                        StructField("alt", IntegerType(), True),
                                        StructField("url", StringType(), True)
])
                

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Select required columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col,lit
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename the required columns

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "lattitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Add ingestion date to dataframe
# MAGIC

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Write the dataframe to the datalake as parquet
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("Success")