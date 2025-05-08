# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: add schema and Read the data from json file 

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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Drop the columns not needed
# MAGIC

# COMMAND ----------


constructors_dropped_df = constructors_df.drop(constructors_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Rename the required columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                                .withColumnRenamed("constructorRef", "constructor_ref") \
                                                .withColumn("data_source", lit(v_data_source))\
                                                .withColumn("file_date", lit(v_file_date))

                                                

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Write the dataframe to the datalake as parquet
# MAGIC

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")