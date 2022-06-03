# Databricks notebook source
# MAGIC %pip install xlrd openpyxl s3fs==2022.3.0

# COMMAND ----------

# MAGIC %md ## Pandas to read excel from DBFS

# COMMAND ----------


import openpyxl
import numpy as np

# COMMAND ----------

pdf = pd.read_excel('/dbfs/FileStore/Online_Retail.xlsx')

# COMMAND ----------

pdf

# COMMAND ----------

# MAGIC %md ## Generate dataset

# COMMAND ----------

from pyspark.sql import DataFrame
import sklearn
import pandas as pd

def regression_data_gen(n_samples=1000, n_features=100, n_informative=10) -> DataFrame:
  x,y = sklearn.datasets.make_regression(n_samples=n_samples, n_features=n_features, n_informative=n_informative)
  pdf = pd.DataFrame(x, dtype=object).rename(lambda x: 'c'+str(x), axis='columns')
  cols = list(pdf.columns)
  pdf['label'] = y
  pdf['index'] = pdf.index
  reordered = ['index','label',*cols]
  #print(reordered)
  #return pdf[reordered]
  return spark.createDataFrame(pdf[reordered])

regression_data_gen(n_samples=1_000_000).display()

# COMMAND ----------

from pyspark.sql import DataFrame
import sklearn
import pandas as pd

def regression_data_gen(n_samples=1000, n_features=100, n_informative=10) -> DataFrame:
  x,y = sklearn.datasets.make_regression(n_samples=n_samples, n_features=n_features, n_informative=n_informative)
  pdf = pd.DataFrame(x, dtype=object).rename(lambda x: 'c'+str(x), axis='columns')
  cols = list(pdf.columns)
  pdf['label'] = y
  pdf['index'] = pdf.index
  reordered = ['index','label',*cols]
  #print(reordered)
  return pdf[reordered]
  #return spark.createDataFrame(pdf).select(reordered)

df = regression_data_gen(n_samples=1_000_000)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id
import numpy as np
import sklearn

def regression_data_gen(n_samples=1000, n_features=100, n_informative=10) -> DataFrame:
  x,y = sklearn.datasets.make_regression(n_samples=n_samples, n_features=n_features, n_informative=n_informative)

  rdd1 = sc.parallelize(np.column_stack((y,x)))
  rdd2 = rdd1.map(lambda x: [float(i) for i in x])
  cols = ['c'+ str(c) for c in range(1,n_features+1)]
  #print(cols)
  df = rdd2.toDF(['label',*cols]).selectExpr('monotonically_increasing_id() as id','*')
  return df

df = regression_data_gen(n_samples=100_000)

# COMMAND ----------

count = df.count()
count

# COMMAND ----------

import pyspark.pandas as ps
import openpyxl
df.to_pandas_on_spark().to_excel(F"s3://oetrta/dmoore/workdir/regression_data_{count}.xlsx")

# COMMAND ----------

# MAGIC %fs ls s3://oetrta/dmoore/workdir

# COMMAND ----------

import openpyxl
df.toPandas().to_excel(F"s3://oetrta/dmoore/workdir/regression_data_{count}_toPandas.xlsx")

# COMMAND ----------

# MAGIC %md ### Test 4

# COMMAND ----------

df = regression_data_gen(n_samples=100_000)
pdf = df.toPandas()

# COMMAND ----------

pdf.to_excel(F"s3://oetrta/dmoore/workdir/regression_data_{count}_toPandas.xlsx")

# COMMAND ----------

# MAGIC %md ### Test 5

# COMMAND ----------

pdf.to_excel(F"/tmp/regression_data_{count}_toPandas.xlsx")
dbutils.fs.cp(F"/tmp/regression_data_{count}_toPandas.xlsx", F"s3://oetrta/dmoore/workdir/regression_data_{count}_toPandas.xlsx")

# COMMAND ----------


