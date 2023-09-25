# Databricks notebook source
path = "/dbfs/FileStore/shared_uploads/douglas.moore@databricks.com/2017_08_03__0006.czi"

# COMMAND ----------

display(dbutils.fs.ls(path.replace("/dbfs","dbfs:")))

# COMMAND ----------


