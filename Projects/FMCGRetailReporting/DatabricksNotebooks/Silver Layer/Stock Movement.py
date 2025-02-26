# Databricks notebook source
# DBTITLE 1,Retrieve load_date using dbutils.widgets.get()
load_date=dbutils.widgets.get('load_date')

# COMMAND ----------

# DBTITLE 1,Importing PySpark Functions
from pyspark.sql.functions import col, lit, coalesce

# COMMAND ----------

# DBTITLE 1,Use retail_store catalog
# MAGIC %sql
# MAGIC use catalog retail_store;

# COMMAND ----------

# DBTITLE 1,Load stock_movement data for specific date
source_df = spark.read.parquet(f'abfss://bronze@devanandstorageaccount.dfs.core.windows.net/stock_movement/{load_date}/*.parquet')

# COMMAND ----------

# DBTITLE 1,Clean up the data
curr_day_df = source_df.withColumn('StockDate',col('Date').cast('date')).withColumn('StoreID',col('StoreID').cast('int')).withColumn('ProductID',col('ProductID').cast('int')).withColumn('OpeningStock',col('OpeningStock').cast('int')).withColumn('ReceivedStock',col('ReceivedStock').cast('int')).withColumn('SoldStock',col('SoldStock').cast('int')).withColumn('ClosingStock',col('ClosingStock').cast('int')).drop('Date')

# COMMAND ----------

# DBTITLE 1,Query previous day stock movement data
prev_day_df = spark.sql(f"select * from silver.stock_movement_fact where StockDate=date_add(to_date('{load_date}','yyyy/MM/dd'), -1)")

# COMMAND ----------

# DBTITLE 1,Calculate the waste stock count
waste_stock_df = curr_day_df.join(prev_day_df,(curr_day_df.StoreID==prev_day_df.StoreID) & (curr_day_df.ProductID==prev_day_df.ProductID),'left').withColumn('WasteStockNew',coalesce((curr_day_df.OpeningStock-prev_day_df.ClosingStock),lit(0))).select(curr_day_df.StockID,curr_day_df.StoreID,curr_day_df.ProductID,curr_day_df.OpeningStock,curr_day_df.ReceivedStock,curr_day_df.SoldStock,curr_day_df.ClosingStock,curr_day_df.StockDate,col('WasteStockNew')).withColumnRenamed('WasteStockNew','WasteStock')

# COMMAND ----------

# DBTITLE 1,Retrieve active products from product dimension
product_df=spark.sql("select * from silver.product_dim where IsActive='Y'")

# COMMAND ----------

# DBTITLE 1,Join waste and product data for loss calculation
tranformed_df = waste_stock_df.join(product_df, (waste_stock_df.ProductID==product_df.ProductID), 'left').withColumn('LossDueToWastage',col('Price')*col('WasteStock')).select(col('StockID'),col('StoreID'),product_df.ProductID.cast('int'),col('OpeningStock'),col('ReceivedStock'),col('SoldStock'),col('ClosingStock'),col('StockDate'),col('WasteStock'),col('LossDueToWastage'))

# COMMAND ----------

# DBTITLE 1,Save Transformed Data to Stock Movement Table
tranformed_df.write.mode('append').saveAsTable('silver.stock_movement_fact')

# COMMAND ----------

# DBTITLE 1,Exit transformed DataFrame count
dbutils.notebook.exit(tranformed_df.count())