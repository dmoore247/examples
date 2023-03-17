# Databricks notebook source
# MAGIC %pip install databricks-sql-connector

# COMMAND ----------

conn_timeout = 5 # seconds
import os
import logging as logger
from time import sleep
from os import getenv
os.environ["DATABRICKS_HOST"] = "e2-demo-field-eng.cloud.databricks.com"
# os.environ["SQL_CONN"] = "/sql/protocolv1/o/1444828305810485/1023-043142-sakfd809"
os.environ["SQL_CONN"] ="/sql/1.0/endpoints/e2132350482b3089"
os.environ["DATABRICKS_TOKEN"] = "x"


# COMMAND ----------

# MAGIC %sh 
# MAGIC apt-get install nmap -y
# MAGIC nmap -sT e2-demo-field-eng.cloud.databricks.com 443

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd /tmp
# MAGIC wget https://e2-demo-field-eng.cloud.databricks.com

# COMMAND ----------

# MAGIC %sh cat index.html

# COMMAND ----------

f = open('/tmp/index.html')
print(f.read())

# COMMAND ----------

from databricks import sql
for s in range(0, conn_timeout):
  while True:
      try:
          connection = sql.connect(
              server_hostname=getenv("DATABRICKS_HOST"),
              http_path=getenv("SQL_CONN"),
              access_token=getenv("DATABRICKS_TOKEN"),
          )
      except Exception as err:
          logger.warning(err)
          logger.info("retrying in 30s")
          sleep(30)
          continue
      break

# COMMAND ----------

connection = sql.connect(
              server_hostname=getenv("DATABRICKS_HOST"),
              http_path=getenv("SQL_CONN"),
              access_token=getenv("DATABRICKS_TOKEN")
          )

# COMMAND ----------

import pandas as pd
import numpy as np
import math

# COMMAND ----------

pdf = pd.read_csv('https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv')
pdf['row_num'] = np.arange(len(pdf))
pdf = pdf[['row_num', 'sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species']]

# COMMAND ----------


pdf

# COMMAND ----------

sdf = spark.createDataFrame(pdf)

# COMMAND ----------

# MAGIC %sql drop table douglas_moore_bronze.iris

# COMMAND ----------

sdf.write.saveAsTable('douglas_moore_bronze.iris')

# COMMAND ----------

# MAGIC %sql show tables in douglas_moore_bronze

# COMMAND ----------

# MAGIC %sql describe table douglas_moore_bronze.iris

# COMMAND ----------

# MAGIC %sql drop table douglas_moore_bronze.iris2

# COMMAND ----------

# MAGIC %sql create table douglas_moore_bronze.iris2 AS select * FROM  douglas_moore_bronze.iris limit 0

# COMMAND ----------

# MAGIC %sql select * from douglas_moore_bronze.iris2

# COMMAND ----------

df = pdf

# COMMAND ----------

df

# COMMAND ----------

import math
batch_size = 1000
num_records = df.shape[0]
batch_num = math.floor(num_records / batch_size)
cursor = connection.cursor()
database_name = 'douglas_moore_bronze'
delta_table_name = 'iris2'

for i in range(batch_num + 1):
  pdf = df.iloc[i * batch_size : (i + 1) * batch_size]
  insert_values = pdf.to_records(index=False, index_dtypes=None, column_dtypes=None).tolist()
  query = f"""MERGE INTO {database_name}.{delta_table_name} as Target
          USING (SELECT * FROM (VALUES {",".join([str(i) for i in insert_values])}) AS s ({key_col}, {",".join(val_cols)})) AS Source
          ON Target.row_num = Source.row_num
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *"""
  cursor.execute(query)

# COMMAND ----------


