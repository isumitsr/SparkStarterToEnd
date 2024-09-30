# Databricks notebook source
# MAGIC %md
# MAGIC # SPARK TRANSFORMATIONS

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

raw_fire_df = spark.read.format("csv") \
    .option("header", "true") \
        .option("inferSchema", "true") \
            .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(raw_fire_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 problems:
# MAGIC 1. Column Names are not standardized
# MAGIC 2. Date fields are of string type

# COMMAND ----------

fire_df_renamed = raw_fire_df.withColumnRenamed("Call Number","CallNumber") \
    .withColumnRenamed("Incident Number","IncidentNumber") \
    .withColumnRenamed("Unit ID","UnitID") \
    .withColumnRenamed("Call Type","CallType") \
    .withColumnRenamed("Call Date","CallDate") \
    .withColumnRenamed("Watch Date","WatchDate") \
    .withColumnRenamed("Call Final Disposition","CallFinalDisposition") \
    .withColumnRenamed("Available DtTm","AvailableDtTm") \
    .withColumnRenamed("Zipcode of Incident","ZipcodeOfIncident") \
    .withColumnRenamed("Station Area", "StationArea") \
    .withColumnRenamed("Final Priority", "FinalPriority") \
    .withColumnRenamed("ALS Unit", "ALSUnit") \
    .withColumnRenamed("Call Type Group", "CallTypeGroup") \
    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch") \
    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
    .withColumnRenamed("Supervisor District", "SupervisorDistrict")
                                    

# COMMAND ----------

display(fire_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Date fields correction

# COMMAND ----------

# Checking data types
fire_df_renamed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CallDate, WatchDate are of date datatype but not the AvailableDtTm

# COMMAND ----------

fire_df_new = fire_df_renamed.withColumn("AvailableDtTm", to_timestamp("AvailableDtTm","MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("Delay", round("Delay", 2))

# COMMAND ----------

display(fire_df_new)

# COMMAND ----------

fire_df_new.printSchema()
