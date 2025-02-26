# Databricks notebook source
# DBTITLE 1,Importing Spark functions for data manipulation
from pyspark.sql.functions import date_format,col,round

# COMMAND ----------

# DBTITLE 1,Set active database to retail store catalog
# MAGIC %sql
# MAGIC use catalog retail_store

# COMMAND ----------

# DBTITLE 1,Querying stock movement data for the past year
stock_movement_df = spark.sql("select * from silver.stock_movement_fact where StockDate>date_add(current_date(), -365)")

# COMMAND ----------

# DBTITLE 1,Querying date dimension table from silver dataset
date_df = spark.sql("select * from silver.date_dim")

# COMMAND ----------

# DBTITLE 1,Calculate the stock wastage for different weekdays
stock_wastage_df = stock_movement_df.join(date_df, stock_movement_df.StockDate == date_df.Date, 'left').select('WasteStock','Weekday').groupBy('Weekday').sum('WasteStock').withColumnRenamed('sum(WasteStock)','TotalWasteStock')

# COMMAND ----------

# DBTITLE 1,Saving stock wastage data to a table
stock_wastage_df.write.mode('overwrite').saveAsTable('gold.weekday_stock_wastage')

# COMMAND ----------

# DBTITLE 1,Filtering active suppliers from supplier dimension
supplier_df = spark.sql("select * from silver.supplier_dim where IsActive='Y'")

# COMMAND ----------

# DBTITLE 1,Querying active products from product dimension
product_df = spark.sql("select * from silver.product_dim where IsActive='Y'")

# COMMAND ----------

# DBTITLE 1,Calculate total stock wastage by supplier
loss_due_to_stock_wastage = stock_movement_df.join(product_df, stock_movement_df.ProductID == product_df.ProductID, 'left').join(supplier_df, product_df.SupplierID == supplier_df.SupplierID, 'left').select(supplier_df.Name,stock_movement_df.LossDueToWastage).groupBy('Name').sum('LossDueToWastage').withColumnRenamed('sum(LossDueToWastage)','TotalLossDuetoStockWastage').withColumn('TotalLossDuetoStockWastage',round(col('TotalLossDuetoStockWastage'),2)).filter(col('TotalLossDuetoStockWastage')>0)

# COMMAND ----------

# DBTITLE 1,Save stock wastage data to table
loss_due_to_stock_wastage.write.mode('overwrite').saveAsTable('gold.loss_due_to_stock_wastage')