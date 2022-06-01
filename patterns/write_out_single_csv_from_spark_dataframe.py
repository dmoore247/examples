# Databricks notebook source
df = spark.range(1000, numPartitions=10)

# COMMAND ----------

spark.conf.set("fs.defaultName","out.csv")
df.to_pandas_on_spark().to_csv("s3://oetrta/dmoore/workdir/my_output.csv", num_files=1)

# COMMAND ----------

# MAGIC %fs ls s3://oetrta/dmoore/workdir

# COMMAND ----------

# MAGIC %fs ls s3://oetrta/dmoore/workdir/my_output.csv/

# COMMAND ----------

# MAGIC %pip install s3fs

# COMMAND ----------

spark.range(1_000_000_000,numPartitions=32).toPandas().to_csv("s3://oetrta/dmoore/workdir/myfile.csv")

# COMMAND ----------

# MAGIC %fs ls s3://oetrta/dmoore/workdir/myfile.csv

# COMMAND ----------


