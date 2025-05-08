# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION 'abfss://demo@dmformula01.dfs.core.windows.net/'

# COMMAND ----------

results_df = spark.read.json(f"{raw_folder_path}/2021-03-28/results.json")
display(results_df)

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").save("abfss://demo@dmformula01.dfs.core.windows.net/results_external")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://demo@dmformula01.dfs.core.windows.net/results_external'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

spark.read.format("delta").load("abfss://demo@dmformula01.dfs.core.windows.net/results_external").display()

# COMMAND ----------

results_df.write.mode("overwrite").format("delta").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC #### UPDATE AND DELETE USING DELTA TABLES
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE  f1_demo.results_managed 
# MAGIC SET points = 11 - position 
# MAGIC WHERE position <= 10

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://demo@dmformula01.dfs.core.windows.net/results_managed")

deltaTable.update("position <= 10", {"points" : "21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed 
# MAGIC WHERE position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://demo@dmformula01.dfs.core.windows.net/results_managed")

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using UPSERT and MERGE

# COMMAND ----------

drivers_day1_df = spark.read\
    .option("inferSchema", True)\
        .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
            .filter("driverId <= 10")\
            .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read\
    .option("inferSchema", True)\
        .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
            .filter("driverId BETWEEN 6 AND 15")\
            .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read\
    .option("inferSchema", True)\
        .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
            .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20")\
            .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### delta lake merge functions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     driverId = upd.driverId,
# MAGIC     dob = upd.dob,
# MAGIC     forename = upd.forename,
# MAGIC     surname = upd.surname,
# MAGIC     updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.driverId,
# MAGIC     upd.dob,
# MAGIC     upd.forename,
# MAGIC     upd.surname,
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     driverId = upd.driverId,
# MAGIC     dob = upd.dob,
# MAGIC     forename = upd.forename,
# MAGIC     surname = upd.surname,
# MAGIC     updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     upd.driverId,
# MAGIC     upd.dob,
# MAGIC     upd.forename,
# MAGIC     upd.surname,
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, "abfss://demo@dmformula01.dfs.core.windows.net/drivers_merge")
deltaTable.alias("tgt") \
  .merge(
    drivers_day3_df.alias('upd'),
    'tgt.driverId = upd.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updatedDate": current_timestamp()
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createdDate": current_timestamp()
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge ORDER BY driverId ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### HISTORY & VERSIONING

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2025-05-04T21:39:35.000+00:00'

# COMMAND ----------

df = spark.read.format("delta").option('timeStampAsOf', '2025-05-04T21:39:35.000+00:00').load("abfss://demo@dmformula01.dfs.core.windows.net/drivers_merge")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### VACCUM

# COMMAND ----------

# MAGIC %md
# MAGIC ##### TIME TRAVEL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge ORDER BY driverId ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 4 src
# MAGIC    ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting Parquet to Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta
# MAGIC

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet").save("abfss://demo@dmformula01.dfs.core.windows.net/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://demo@dmformula01.dfs.core.windows.net/drivers_convert_to_delta_new`