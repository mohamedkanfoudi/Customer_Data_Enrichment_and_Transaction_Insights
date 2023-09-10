# Databricks notebook source
service_credential = dbutils.secrets.get(scope="Secret02",key="ClientSecret")

spark.conf.set("fs.azure.account.auth.type.azureprojectmk2023.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azureprojectmk2023.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azureprojectmk2023.dfs.core.windows.net", "85c030e5-d1af-4660-961f-e52428c3133f")
spark.conf.set("fs.azure.account.oauth2.client.secret.azureprojectmk2023.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azureprojectmk2023.dfs.core.windows.net", "https://login.microsoftonline.com/d001e3df-c491-489f-9fe5-7e6bdf26c1aa/oauth2/token")

# COMMAND ----------

bronze_folder_path  = "abfss://bronze@azureprojectmk2023.dfs.core.windows.net/"
silver_folder_path= "abfss://silver@azureprojectmk2023.dfs.core.windows.net/"
gold_folder_path = "abfss://gold@azureprojectmk2023.dfs.core.windows.net/"
