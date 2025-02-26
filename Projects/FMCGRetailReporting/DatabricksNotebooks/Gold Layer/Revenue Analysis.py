# Databricks notebook source
# DBTITLE 1,Importing necessary functions for data manipulation
from pyspark.sql.functions import date_format,col,round

# COMMAND ----------

# DBTITLE 1,Set active database to retail store catalog
# MAGIC %sql
# MAGIC use catalog retail_store

# COMMAND ----------

# DBTITLE 1,Filtering sales transactions for the past year

sales_transaction_df = spark.sql("select * from silver.sales_transaction_fact where TransactionDate>date_add(current_date(), -365)")

# COMMAND ----------

# DBTITLE 1,Retrieving active products from product dimension
product_df = spark.sql("select * from silver.product_dim where IsActive='Y'")

# COMMAND ----------

# DBTITLE 1,Filtering active stores from store dimension
store_df = spark.sql("select * from silver.store_dim where IsActive='Y'")

# COMMAND ----------

# DBTITLE 1,Join with dimension tables to get the necessary columns
final_df = sales_transaction_df.join(product_df,'ProductID','left').join(store_df,'StoreID','left').select(sales_transaction_df['*'],product_df['Category'].alias('ProductCategoryName'),store_df['City'].alias('CityName')).withColumn('Month',date_format(sales_transaction_df['TransactionDate'],'yyyyMM'))

# COMMAND ----------

# DBTITLE 1,Saving monthly revenue data to gold database
product_revenue = final_df.groupBy('Month').sum('TotalAmount').withColumnRenamed('sum(TotalAmount)','MonthlyRevenue').withColumn('MonthlyRevenue',round(col('MonthlyRevenue'),2))
product_revenue.write.mode('overwrite').saveAsTable('gold.monthly_revenue')

# COMMAND ----------

# DBTITLE 1,Summarizing and saving product revenue data
product_revenue = final_df.groupBy('ProductCategoryName').sum('TotalAmount').withColumnRenamed('sum(TotalAmount)','Revenue').withColumn('Revenue',round(col('Revenue'),2))
product_revenue.write.mode('overwrite').saveAsTable('gold.product_revenue')

# COMMAND ----------

# DBTITLE 1,Calculate top 5 city revenues and save as table
store_revenue = final_df.groupBy('CityName').sum('TotalAmount').withColumnRenamed('sum(TotalAmount)','Revenue').withColumn('Revenue',round(col('Revenue'),2))
store_revenue.orderBy('Revenue').limit(5).write.mode('overwrite').saveAsTable('gold.city_revenue')