# Databricks notebook source
# DBTITLE 1,Read the required parameters
pipeline_run_id=dbutils.widgets.get('pipeline_run_id')
pipeline_status=dbutils.widgets.get('pipeline_status')
entity=dbutils.widgets.get('entity')
records_processed=dbutils.widgets.get('records_processed')
end_time=dbutils.widgets.get('end_time')
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

spark.sql(f"UPDATE PIPELINE_RUN_LOG SET PIPELINE_STATUS='{pipeline_status}', RECORDS_PROCESSED='{records_processed}', END_TIME='{end_time}' WHERE PIPELINE_RUN_ID='{pipeline_run_id}' and ENTITY='{entity}' and PIPELINE_STAGE='{pipeline_stage}'")