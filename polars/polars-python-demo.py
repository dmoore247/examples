# Databricks notebook source
# MAGIC %pip install polars pyarrow[parquet] s3fs fsspec

# COMMAND ----------

import s3fs

# COMMAND ----------

# MAGIC %sql LIST 's3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/'

# COMMAND ----------

# MAGIC %sql LIST 's3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/one_billion'

# COMMAND ----------

# MAGIC %fs ls s3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/big6

# COMMAND ----------

path = 's3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/big6'

# COMMAND ----------

spark.range(300_000_000).selectExpr("id", "cast(id as string) as id_str", "reverse(cast(id as string)) as reverse_id", "now() as update_ts").write.mode('overwrite').format('parquet').save(path)

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


