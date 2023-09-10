# Databricks notebook source
dbutils.widgets.text('p_file_date', '2022-09-10')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import *

# COMMAND ----------

customerDrivers_df = spark.read.format("delta").load(f"{silver_folder_path}customerDrivers" )


# COMMAND ----------

purchaseTrx_df = spark.read.format("delta").load(f"{silver_folder_path}purchaseTrx" )


# COMMAND ----------

display(purchaseTrx_df.alias('p').join(customerDrivers_df.alias('c'), (col('p.customer_id') == col('c.customer_id')) & (col('p.date') == col('c.date')) , "inner" ))

# COMMAND ----------

featurePurchaseTrx_df = purchaseTrx_df.alias('p').join(customerDrivers_df.alias('c'), (col('p.customer_id') == col('c.customer_id')) & (col('p.date') == col('c.date')) , "inner" ).select(col('p.date'),col('p.customer_id'),col('p.Period'),col('p.purchase_amount'),col('p.currency_type'),col('p.evaluation_channel'),col('p.rate'),col('c.monthly_salary'),col('c.health_score'),col('c.withdrawal_money'),col('c.category'),col('c.is_risk_customer'),)

# COMMAND ----------

featurePurchaseTrx_df = add_inser_timestamp(featurePurchaseTrx_df)

# COMMAND ----------

featurePurchaseTrx_df.write.format("delta").mode("overwrite").save(gold_folder_path+"featurePurchaseTrx")

# COMMAND ----------

aggPurchaseTrx = featurePurchaseTrx_df.groupBy('date' , 'Period' , 'currency_type' ,'evaluation_channel' , 'category' )\
    .agg(sum('purchase_amount').alias('sum_purchase_amount') ,avg('purchase_amount').alias('avg_purchase_amount') , sum('withdrawal_money').alias('sum_withdrawal_money') ,avg('withdrawal_money').alias('avg_withdrawal_money'), max('rate').alias ('max_rate') ,min('rate').alias('min_rate'), avg('health_score').alias('avg_health_score') ,avg('monthly_salary').alias('avg_monthly_salary')).orderBy('date' , 'Period' , 'evaluation_channel' ,'category' , 'currency_type'  )

# COMMAND ----------

aggPurchaseTrx = add_inser_timestamp(aggPurchaseTrx)

# COMMAND ----------

aggPurchaseTrx.write.format("delta").mode("overwrite").save(gold_folder_path+"aggPurchaseTrx")

# COMMAND ----------

