# Databricks notebook source
# DBTITLE 1,Importing PySpark functions for data manipulation
from pyspark.sql.functions import date_format,col,round

# COMMAND ----------

# DBTITLE 1,SQL Use Catalog for Retail Store
# MAGIC %sql
# MAGIC use catalog retail_store

# COMMAND ----------

# DBTITLE 1,Querying sales transactions data for the past year

sales_transaction_df = spark.sql("select * from silver.sales_transaction_fact where TransactionDate>date_add(current_date(), -365)")

# COMMAND ----------

# DBTITLE 1,Top Customers Loyalty Scores Analysis
customer_loyalty_df = sales_transaction_df.groupBy('CustomerID').sum('IsHighestPurchaseForTheDay').withColumnRenamed('sum(IsHighestPurchaseForTheDay)','CustomerLoyaltyScore').orderBy(col('CustomerLoyaltyScore').desc()).limit(10)

# COMMAND ----------

# DBTITLE 1,Active Customers in Customer Dimension
customer_df = spark.sql("select * from silver.customer_dim where IsActive='Y'")

# COMMAND ----------

# DBTITLE 1,Joining Customer Loyalty Score with Customer Name
final_df = customer_loyalty_df.join(customer_df, customer_loyalty_df.CustomerID == customer_df.CustomerID, 'left').select(customer_df.Name, customer_loyalty_df.CustomerLoyaltyScore)

# COMMAND ----------

# DBTITLE 1,Saving final DataFrame to customer loyalty score table
final_df.write.mode('overwrite').saveAsTable('gold.customer_loyalty_score')