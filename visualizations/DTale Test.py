# Databricks notebook source
# MAGIC %pip install dtale

# COMMAND ----------

import dtale
import logging
import pandas as pd
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j").setLevel(logging.ERROR)

df = pd.DataFrame([dict(a=1,b=2,c=3)])

d = dtale.show(df, ignore_duplicate=True)

# COMMAND ----------

# Accessing data associated with D-Tale process
tmp = d.data.copy()

# COMMAND ----------

tmp

# COMMAND ----------

d._data_id  # the process's data identifier

# COMMAND ----------

d._url  # the url to access the process

# COMMAND ----------

# Configure location of loanstats_2012_2017.parquet
path = "/databricks-datasets/samples/lending_club/parquet/"

# Read loanstats_2012_2017.parquet
data = spark.read.parquet(path)

display(data)

# COMMAND ----------

d2 = dtale.show(data.toPandas(), ignore_duplicate=True)

# COMMAND ----------

d2.main_url()

# COMMAND ----------

d2.open_browser()

# COMMAND ----------


