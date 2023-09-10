# Databricks notebook source
dbutils.widgets.text('p_file_date', '2022-09-10')
#v_file_date = dbutils.widgets.get('p_file_date')
v_file_date = '2022-09-10'

# COMMAND ----------

# MAGIC %run "../includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType,BooleanType

from pyspark.sql.functions import substring, isnotnull , col

# COMMAND ----------

schema = StructType(fields=[
    StructField("customerId", StringType(), False),
    StructField("firstName", StringType(), False),
    StructField("lastName", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("email", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("is_active", StringType(), True)
])

# COMMAND ----------

customer_df = spark.read.option("header", True).schema(schema).csv(bronze_folder_path+"customer/customer_"+v_file_date+".csv")

# COMMAND ----------

tmp_customer_df = customer_df.withColumnRenamed("customerId" , "customer_id").withColumnRenamed("firstName" , "first_name").withColumnRenamed("lastName" , "last_name")

# COMMAND ----------

tmp_customer_df = tmp_customer_df.withColumn("gender" , substring("gender" , 1,1))

# COMMAND ----------

final_df = add_inser_timestamp(tmp_customer_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").save(silver_folder_path+"customer")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

