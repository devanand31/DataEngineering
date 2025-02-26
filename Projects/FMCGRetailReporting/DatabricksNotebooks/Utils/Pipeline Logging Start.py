# Databricks notebook source
# DBTITLE 1,Read the required parameters
pipeline_run_id=dbutils.widgets.get('pipeline_run_id')
entity=dbutils.widgets.get('entity')
pipeline_name=dbutils.widgets.get('pipeline_name')
start_time=dbutils.widgets.get('start_time')
pipeline_stage=dbutils.widgets.get('pipeline_stage')

# COMMAND ----------

# DBTITLE 1,Set Catalog
# MAGIC %sql
# MAGIC USE CATALOG UTILS;

# COMMAND ----------

# DBTITLE 1,Set Database
# MAGIC %sql
# MAGIC USE DATABASE LOGGING;

# COMMAND ----------

spark.sql(f"INSERT INTO pipeline_run_log VALUES('{pipeline_run_id}','{entity}','{pipeline_name}','{pipeline_stage}','RUNNING',NULL,'{start_time}',NULL)")