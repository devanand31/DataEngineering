# Databricks notebook source
# DBTITLE 1,Read the load date
load_date=dbutils.widgets.get('load_date')

# COMMAND ----------

# DBTITLE 1,PySpark SQL Functions and Window Import
from pyspark.sql.functions import col, sum, round, dense_rank, when
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Switch to Retail Store Catalog
# MAGIC %sql
# MAGIC use catalog retail_store;

# COMMAND ----------

# DBTITLE 1,Load sales transactions data from Azure Data Lake
source_df = spark.read.parquet(f'abfss://bronze@devanandstorageaccount.dfs.core.windows.net/sales_transactions/{load_date}/*.parquet')

# COMMAND ----------

# DBTITLE 1,Clean up the data
cleaned_df = source_df.withColumn('TransactionDate',col('Date').cast('date')).withColumn('CustomerID',col('CustomerID').cast('int')).withColumn('StoreID',col('StoreID').cast('int')).withColumn('ProductID',col('ProductID').cast('int')).withColumn('Quantity',col('Quantity').cast('int')).withColumn('TotalAmount',col('TotalAmount').cast('float')).drop('Date')

# COMMAND ----------

# DBTITLE 1,Calculate the Most number of Highest Transaction customers
tranformed_df = cleaned_df.withColumn('PurchaseRankForTheDay',dense_rank().over(Window.partitionBy(col('TransactionDate')).orderBy(col('TotalAmount').desc()))).withColumn('IsHighestPurchaseForTheDay',when(col('PurchaseRankForTheDay')<=3,1).otherwise(0)).drop('PurchaseRankForTheDay')

# COMMAND ----------

# DBTITLE 1,Delete if already the incremental data is loaded
spark.sql(f"delete from silver.sales_transaction_fact WHERE TransactionDate=CAST('{load_date}' AS DATE)")

# COMMAND ----------

# DBTITLE 1,Append the data to the final table
tranformed_df.write.mode('append').saveAsTable('silver.sales_transaction_fact')

# COMMAND ----------

# DBTITLE 1,Exit Transformed Data Count
dbutils.notebook.exit(tranformed_df.count())