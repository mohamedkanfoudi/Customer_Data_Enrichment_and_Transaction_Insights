# Databricks notebook source
dbutils.widgets.text('p_file_date', '2022-09-10')
#v_file_date = dbutils.widgets.get('p_file_date')
v_file_date = '2022-09-10'

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType


# COMMAND ----------

schema = StructType(fields=[
    StructField("date", DateType(), False),
    StructField("customerId", StringType(), False),
    StructField("Period", IntegerType(), False),
    StructField("purchaseAmount", DoubleType(), False),
    StructField("currencyType", StringType(), False),
    StructField("evaluationChannel", StringType(), False),
    StructField("rate", DoubleType(), False)
])


# COMMAND ----------

purchaseTrx_df = spark.read.option("header", True).schema(schema).csv(bronze_folder_path+"customer/customer_" +v_file_date+".csv")

# COMMAND ----------

final_df = purchaseTrx_df.withColumnRenamed("customerId" , "customer_id").withColumnRenamed("purchaseAmount" , "purchase_amount").withColumnRenamed('currencyType' , 'currency_type').withColumnRenamed('evaluationChannel' , 'evaluation_channel') 

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").save(silver_folder_path+"purchaseTrx")

# COMMAND ----------

