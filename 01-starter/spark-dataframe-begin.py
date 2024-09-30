# Databricks notebook source
fire_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(fire_df)

# COMMAND ----------

fire_df.createGlobalTempView("vw_fire_service_calls")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.vw_fire_service_calls
