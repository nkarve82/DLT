# Databricks notebook source
# MAGIC %md ## Delta Live Table Event Log Examples
# MAGIC * Query the Delta Live Table Event Log to view expecataions and lineage 

# COMMAND ----------

# Fill in the pipelines_id and pipeline_name
pipelines_id = "bc91a642-95e9-4661-8b8c-00a4272bbeff"
pipeline_name = "Pipeline_DLT_Demo_Wiki"

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md ### Query Expectations Data Quality Metrics Sample

# COMMAND ----------

# Data Quality Expecations | Flow Progress Completed
sqlQuery = """SELECT id, origin, timestamp, details
                FROM delta.`dbfs:/pipelines/""" + pipelines_id + """/system/events/`
               WHERE details LIKE '%flow_progress%COMPLETED%data_quality%expectations%' order by timestamp desc"""
df = spark.sql(sqlQuery)


df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from df

# COMMAND ----------

|# Data Quality Expecations | Flow Progress Completed
sqlQuery = """SELECT id, origin, timestamp, details
                FROM delta.`dbfs:/pipelines/""" + pipelines_id + """/system/events/`
               WHERE details LIKE '%flow_progress%COMPLETED%data_quality%expectations%' order by timestamp desc"""
df = spark.sql(sqlQuery)


df_expectations.createOrReplaceTempView("df_expectations")

# Define DQ Expectations schema
schema = schema_of_json("""
  {"flow_progress":{
    "status":"COMPLETED",
    "metrics":{"num_output_rows":91939},
    "data_quality":{"dropped_records":32,
    "expectations":[
      {"name":"non zero passenger count",
       "dataset":"silver_GreenCab",
       "passed_records":91939,
       "failed_records":32}
     ]}}
  }""")      

# Expectations DataFrame
df_expectations = df.withColumn("details_json", from_json(df.details, schema))
df_expectations.createOrReplaceTempView("df_expectations")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, timestamp, details_json.flow_progress.data_quality, origin, details_json FROM df_expectations ORDER BY timestamp DESC LIMIT 20

# COMMAND ----------

# MAGIC %md ## Review DLT Lineage

# COMMAND ----------

# DLT Lineage (skip maintenance jobs)
sqlQuery = """SELECT id, origin, sequence, timestamp, message, event_type, details
                FROM delta.`dbfs:/pipelines/""" + pipelines_id + """/system/events/`
               WHERE origin.cluster_id = (
                 SELECT origin.cluster_id FROM delta.`dbfs:/pipelines/""" + pipelines_id + """/system/events/`
                  WHERE origin.pipeline_name = '""" + pipeline_name + """'
                    AND origin.maintenance_id IS NULL ORDER BY timestamp DESC LIMIT 1
               )"""
df = spark.sql(sqlQuery)

# Define DQ Expectations schema
schema = schema_of_json("""
  {"flow_progress":{
    "status":"COMPLETED",
    "metrics":{"num_output_rows":91939},
    "data_quality":{"dropped_records":32,
    "expectations":[
      {"name":"non zero passenger count",
       "dataset":"silver_GreenCab",
       "passed_records":91939,
       "failed_records":32}
     ]}}
  }""")      

# Expectations DataFrame
df_lineage = df.withColumn("details_json", from_json(df.details, schema))
df_lineage.createOrReplaceTempView("df_lineage")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, timestamp, sequence.data_plane_id.seq_no, message, event_type, details_json  FROM df_lineage

# COMMAND ----------


