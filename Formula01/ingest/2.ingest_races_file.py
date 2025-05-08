# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest races.csv file

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

races_schema = StructType(fields= [StructField("raceId", IntegerType(), False),
                                        StructField("year", IntegerType(), True),
                                        StructField("round", IntegerType(), True),
                                        StructField("circuitId", IntegerType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("date", StringType(), True),
                                        StructField("time", StringType(), True),
                                        StructField("url", StringType(), True)
])
                

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Select required columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename the required columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id") \
.withColumn("data_source", lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Add ingestion date to dataframe
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import concat, lit, to_timestamp

# COMMAND ----------

races_ingested_df = add_ingestion_date(races_renamed_df)

# COMMAND ----------

races_final_df = races_ingested_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))\
    .drop(col("date"), col("time"))
        

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Write the dataframe to the datalake as parquet
# MAGIC

# COMMAND ----------

races_final_df.write.mode("overwrite")\
    .partitionBy("race_year") \
    .format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")