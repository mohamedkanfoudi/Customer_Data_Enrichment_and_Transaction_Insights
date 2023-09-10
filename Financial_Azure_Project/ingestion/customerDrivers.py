# Databricks notebook source
dbutils.widgets.text('p_file_date', '2022-09-10')
#v_file_date = dbutils.widgets.get('p_file_date')
v_file_date = '2022-09-10'

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col

# COMMAND ----------

schema = StructType(fields=[
    StructField("date", DateType(), False),
    StructField("customerId", StringType(), False),
    StructField("monthly_salary", DoubleType(), True),
    StructField("health_score", IntegerType(), True),
    StructField("withdrawal_money", DoubleType(), True),
    StructField("category", StringType(), True),
])

# COMMAND ----------

customerDrivers_df = spark.read. \
                        option("header", True). \
                        schema(schema). \
                        csv(bronze_folder_path+"customer/customer_"+v_file_date+".csv")


# COMMAND ----------

tmp_customerDrivers = customerDrivers_df.fillna({
    'monthly_salary' : 3000,
    'health_score' : 100,
    'withdrawal_money' : 0,
    'category' : 'OTHERS'
})

# COMMAND ----------

final_df = tmp_customerDrivers.withColumnRenamed("customerId", "customer_id").withColumn("is_risk_customer" , col("health_score") < 100)

# COMMAND ----------

final_df.write.format("delta").mode("overwrite").save(silver_folder_path+"customerDrivers")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

