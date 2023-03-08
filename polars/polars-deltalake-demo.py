# Databricks notebook source
# MAGIC %pip install awscli DeltaLake==0.6.2 #polars

# COMMAND ----------

path = 's3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/big12'
#spark.range(10_000_000).selectExpr("id", "cast(id as string) as id_str", "reverse(cast(id as string)) as reverse_id", "now() as update_ts").write.mode('overwrite').format('delta').save(path)

# COMMAND ----------

# MAGIC %sh 
# MAGIC aws s3 cp s3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/big9/_delta_log/00000000000000000000.json .
# MAGIC cat 00000000000000000000.json

# COMMAND ----------

# Import Delta Table
from deltalake import DeltaTable

# COMMAND ----------

dt = DeltaTable(path)

# COMMAND ----------

import polars as pl

# Import Delta Table
from deltalake import DeltaTable

# Read the Delta Table using the Rust API
dt = DeltaTable(path)

# Create a Polars Dataframe by initially converting the Delta Lake
# table into a PyArrow table
df = pl.DataFrame(dt.to_pyarrow_table())

# COMMAND ----------

df.filter(pl.col("id") == 61780

# COMMAND ----------

import s3fs

# COMMAND ----------

# MAGIC %sql LIST 's3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/'

# COMMAND ----------

# MAGIC %sql LIST 's3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/one_billion'

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import polars as pl
import pyarrow.parquet as pq
import s3fs

fs = s3fs.S3FileSystem()
print(path)
dataset = pq.ParquetDataset(path, filesystem=fs)
df = pl.from_arrow(dataset.read())

# COMMAND ----------

df.filter(pl.col("id") == 61780)

# COMMAND ----------

#id lookup
df.filter(pl.col("id") == 16780)

# COMMAND ----------


