# Databricks notebook source
# MAGIC %md
# MAGIC # Geoviews - Install

# COMMAND ----------

# MAGIC %sh
# MAGIC apt-get install libproj-dev proj-data proj-bin -y
# MAGIC apt-get install libgeos-dev -y
# MAGIC echo "---------  done  ---------"

# COMMAND ----------

# DBTITLE 1,Install geo packages from conda-forge
# MAGIC %sh 
# MAGIC conda install -c conda-forge cython cartopy geoviews geopandas iris xesmf osmnx -y 
# MAGIC echo "---------  done  ---------"

# COMMAND ----------

# DBTITLE 1,Install data files
# MAGIC %sh 
# MAGIC geoviews examples 
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh find /databricks -name '*libkea*'

# COMMAND ----------

import geopandas

# COMMAND ----------

import cartopy

# COMMAND ----------


