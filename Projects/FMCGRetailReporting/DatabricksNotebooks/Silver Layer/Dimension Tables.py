# Databricks notebook source
# DBTITLE 1,Read the required parameters
load_date=dbutils.widgets.get('load_date')
entity=dbutils.widgets.get('entity')
key_column=dbutils.widgets.get('key_column')
mutable_columns=dbutils.widgets.get('mutable_columns')
full_load_ind=int(dbutils.widgets.get('full_load_ind'))

# COMMAND ----------

# DBTITLE 1,Import libraries
from pyspark.sql.functions import col, dateadd, current_date,lit

# COMMAND ----------

# DBTITLE 1,Set Catalog
# MAGIC %sql
# MAGIC use catalog retail_store;

# COMMAND ----------

# DBTITLE 1,Read the parquet file from the bronze layer
source=spark.read.parquet(f'abfss://bronze@devanandstorageaccount.dfs.core.windows.net/{entity}/{entity}_{load_date}.parquet')

# COMMAND ----------

# DBTITLE 1,Cast the columns to the required types
for cols in spark.sql(f"select column_name,full_data_type from information_schema.columns where table_schema='silver' and table_name='{entity}_dim' and column_name not in ('FromEffectiveDate','ToEffectiveDate','IsActive') order by ordinal_position").collect():
    source=source.withColumn(cols.column_name,col(cols.column_name).cast(cols.full_data_type))

# COMMAND ----------

# DBTITLE 1,Logic for full load
if full_load_ind==1:
    source.withColumn('FromEffectiveDate',current_date()).withColumn('ToEffectiveDate',lit(None).cast('date')).withColumn('IsActive',lit('Y')).write.mode('overwrite').saveAsTable(f'silver.{entity}_dim')
    dbutils.notebook.exit(source.count())

# COMMAND ----------

# DBTITLE 1,Filter the latest records from the file as the file contains entire source data
source_latest=source.filter(col('LastUpdated').cast('date')>=dateadd(lit(load_date).cast('date'),-1))
source_latest.createOrReplaceTempView(f'{entity}_latest')

# COMMAND ----------

# DBTITLE 1,Build the strings to form the query dynamically
select_column_list = spark.sql(f"select concat_ws(',',collect_list(column_name)) from (select column_name from information_schema.columns where table_schema='silver' and table_name='{entity}_dim' order by ordinal_position)").collect()[0][0].split(',')

mutable_columns_list = mutable_columns.split(',')
select_column_string_temp = ','.join(['b.'+i if i in mutable_columns_list else 'a.'+i for i in select_column_list])
select_column_string = select_column_string_temp.replace('a.FromEffectiveDate','current_date() as FromEffectiveDate').replace('a.ToEffectiveDate','CAST(NULL AS DATE) as ToEffectiveDate').replace('a.IsActive',"'Y' as IsActive")

filter_condition_string = ' or '.join(['a.'+i+'<>b.'+i for i in mutable_columns_list])

# COMMAND ----------

# DBTITLE 1,Fetch the new records into a temp table
spark.sql(f"""
SELECT {select_column_string}
FROM silver.{entity}_dim a
INNER JOIN {entity}_LATEST b ON (a.{key_column}=b.{key_column})
WHERE a.IsActive='Y'
and ({filter_condition_string});
""").write.mode('overwrite').saveAsTable(f'temp.{entity}_new_data')

# COMMAND ----------

# DBTITLE 1,Update the records that are changed
update_record_count = spark.sql(f"""
UPDATE silver.{entity}_dim SET ToEffectiveDate=CURRENT_DATE(), IsActive='N' WHERE IsActive='Y' AND {key_column} IN (
SELECT a.{key_column}
FROM silver.{entity}_dim a
INNER JOIN {entity}_latest b ON (a.{key_column}=b.{key_column})
WHERE a.IsActive='Y'
and ({filter_condition_string})) 
""").collect()[0][0]

# COMMAND ----------

# DBTITLE 1,Insert the new records from temp table
new_record_count = spark.sql(f"""
INSERT INTO silver.{entity}_dim
SELECT * FROM temp.{entity}_new_data
""").collect()[0][1]

# COMMAND ----------

# DBTITLE 1,Exit with processed record count for logging
dbutils.notebook.exit(update_record_count+new_record_count)