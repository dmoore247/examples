# Databricks notebook source
# MAGIC %md
# MAGIC # Datashader - Install
# MAGIC ![](https://github.com/holoviz/datashader/blob/master/examples/assets/images/usa_census.jpg?raw=true)
# MAGIC
# MAGIC Last tested with 
# MAGIC `6.4 ML (includes Apache Spark 2.4.5, Scala 2.11)`

# COMMAND ----------

# DBTITLE 1,Download and install census2010 data
# MAGIC %sh
# MAGIC cd /tmp
# MAGIC wget http://s3.amazonaws.com/datashader-data/census2010.parq.zip
# MAGIC unzip /tmp/census2010.parq.zip
# MAGIC cp -r census2010.parq /dbfs/ml/census2010.parq

# COMMAND ----------

# MAGIC %md Cluster libraries
# MAGIC * bokeh==1.4.0
# MAGIC * datashader
# MAGIC * fastparquet
# MAGIC * holoviews
# MAGIC * python-snappy
