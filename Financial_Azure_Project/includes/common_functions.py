# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_inser_timestamp(input_df):
    output_df = input_df.withColumn('inser_timestamp' , current_timestamp())
    return output_df