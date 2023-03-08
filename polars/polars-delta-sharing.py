# Databricks notebook source
# MAGIC %pip install delta-sharing polars[all]

# COMMAND ----------

import delta_sharing
import polars as pl

# COMMAND ----------

client = delta_sharing.SharingClient(profile="/dbfs/shares/dm_gold/gold.share")
table_url = "/dbfs/shares/dm_gold/gold.share#douglas_moore_gold.douglas_moore_gold.big12"
pdf = delta_sharing.load_as_pandas(table_url)
pldf = pl.from_pandas(pdf)
pldf.filter(pl.col("id") == 532)

# COMMAND ----------

pldf.filter(pl.col("id") == 339222)

# COMMAND ----------

pldf.describe()

# COMMAND ----------


