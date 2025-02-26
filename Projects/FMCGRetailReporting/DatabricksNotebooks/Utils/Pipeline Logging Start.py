# Databricks notebook source
pipeline_run_id=dbutils.widgets.get('pipeline_run_id')
entity=dbutils.widgets.get('entity')
pipeline_name=dbutils.widgets.get('pipeline_name')
start_time=dbutils.widgets.get('start_time')
pipeline_stage=dbutils.widgets.get('pipeline_stage')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG UTILS;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE LOGGING;

# COMMAND ----------

spark.sql(f"INSERT INTO pipeline_run_log VALUES('{pipeline_run_id}','{entity}','{pipeline_name}','{pipeline_stage}','RUNNING',NULL,'{start_time}',NULL)")