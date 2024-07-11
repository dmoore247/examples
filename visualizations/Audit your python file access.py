# Databricks notebook source
# MAGIC %sh
# MAGIC wget -v https://vega.github.io/vega-datasets/data/zipcodes.csv -O /Volumes/douglas_moore/demo/visuals/zipcodes.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM system.access.audit 
# MAGIC WHERE service_name LIKE "filesystem" 
# MAGIC --AND action_name = "filesGet" 
# MAGIC AND request_params.path LIKE "/Volumes/douglas_moore/demo/visuals%"

# COMMAND ----------

path = "/Volumes/douglas_moore/demo/visuals/zipcodes.csv"
with open(path,mode='r') as f:
  f.read()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM system.access.audit 
# MAGIC WHERE service_name LIKE "filesystem" 
# MAGIC --AND action_name = "filesGet" 
# MAGIC AND request_params.path LIKE "/Volumes/douglas_moore/demo/visuals%"

# COMMAND ----------


