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

# COMMAND ----------

# MAGIC %md
# MAGIC # CACHE DATAFRAME MEMORY !! - WHY?
# MAGIC ### Since we will be running more analysis on our dataframe and will be using the dataframe often, so we will cache the dataframe in our memory to speed up the execution

# COMMAND ----------

fire_df_new.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analysis of the data using PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC - ##### select count(distinct CallType) as Distinct_callType from demo_db.tbl_fire_service_calls where CallType IS NOT NULL

# COMMAND ----------

# Way 1
fire_df_new.createOrReplaceTempView("fire_service_calls_vw")
q1_df = spark.sql("""select count(distinct CallType) as Distinct_callType from demo_db.tbl_fire_service_calls where CallType IS NOT NULL
                  """)
display(q1_df)

# COMMAND ----------

# Way 2
q1_df = fire_df_new.where("CallType IS NOT NULL") \
    .select("CallType") \
    .distinct().count()
print(q1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC select distinct CallType as Distinct_callType_list from demo_db.tbl_fire_service_calls where CallType IS NOT NULL

# COMMAND ----------

q2_df = fire_df_new.where("CallType IS NOT NULL") \
    .select(expr("CallType as distinct_callType")) \
    .distinct()
display(q2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC select CallNumber, Delay from demo_db.tbl_fire_service_calls where delay > 5

# COMMAND ----------

q3_df = fire_df_new.where("Delay > 5") \
    .select("CallNumber", "Delay")
display(q3_df)

# COMMAND ----------

# MAGIC %md
# MAGIC select CallType, count(CallType) from demo_db.tbl_fire_service_calls group by CallType order by count(CallType) DESC

# COMMAND ----------

q4_df = fire_df_new.select("CallType") \
    .where("CallType IS NOT NULL") \
    .groupBy("CallType") \
    .count() \
    .orderBy("count", ascending=False)

display(q4_df)

# COMMAND ----------

# MAGIC %md
# MAGIC select Calltype, Zipcode, count(*) from demo_db.tbl_fire_service_calls group by Zipcode,calltype order by count(CallType) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC select distinct zipcode, neighborhood from demo_db.tbl_fire_service_calls where Zipcode IN ('94102','94103')

# COMMAND ----------

# MAGIC %md
# MAGIC select sum(numAlarms), avg(Delay), min(Delay), max(Delay) from demo_db.tbl_fire_service_calls

# COMMAND ----------

# MAGIC %md
# MAGIC select distinct YEAR(cast(callDate as date)) from demo_db.tbl_fire_service_calls

# COMMAND ----------

# MAGIC %md
# MAGIC select weekofyear(to_date(callDate, "yyyy-MM-dd")) week_year, count(*) as count
# MAGIC from demo_db.tbl_fire_service_calls
# MAGIC where year(to_date(callDate, "yyyy-MM-dd")) == 2018
# MAGIC group by week_year
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %md
# MAGIC select Neighborhood, delay from demo_db.tbl_fire_service_calls where year(to_date(callDate, "yyyy-MM-dd")) = 2018
# MAGIC order by delay desc
