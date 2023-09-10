# Databricks notebook source
# MAGIC %run "../includes/configurations"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType
from delta.tables import DeltaTable

# COMMAND ----------

customer_df = spark.read.format("delta").load(silver_folder_path+"customer")

# COMMAND ----------

tmp_customer = customer_df.select('customer_id' , 'first_name' , 'last_name' , 'phone' , 'email' , 'gender' , 'is_active')

# COMMAND ----------

tmp_customer = add_inser_timestamp(tmp_customer)

# COMMAND ----------

folder_path = f"{gold_folder_path}customer"

# COMMAND ----------

if(DeltaTable.isDeltaTable(spark,folder_path)):
    deltaTable = DeltaTable.forPath(spark,folder_path)
    dfUpdates = tmp_customer

    deltaTable.alias('src').merge(dfUpadates.alias('upd'), "src.customer_id = upd.customer_id" ).whenMatchedUpdateAll().whenMatchedInsertAll().execute()
else : 
    tmp_customer.write.mode("overwrite").format("delta").save(folder_path)

# COMMAND ----------

spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS demo.customer USING DELTA LOCATION '{gold_folder_path}/customer'")


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

