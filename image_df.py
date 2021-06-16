# Databricks notebook source
path = "/FileStore/plots"
image_df = spark.read.format('image').load(path)

# COMMAND ----------

display(image_df.limit(10))

# COMMAND ----------


