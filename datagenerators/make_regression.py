# Databricks notebook source
# MAGIC %md # Data Generator: make_regression
# MAGIC @author: Douglas Moore
# MAGIC 
# MAGIC @tags: data generator, sklearn, ml, spark, dataframe

# COMMAND ----------

from pyspark.sql import DataFrame
import sklearn
import pandas as pd

def make_regression(n_samples=1000, n_features=100, n_informative=10) -> DataFrame:
  """
    create a dense data set usable in regression modeling and load testing.
    The dataset will have an unique integer identifier column, a label column and floating point feature columns.
    This generator is based on the sklearn make_regression
  """
  x,y = sklearn.datasets.make_regression(n_samples=n_samples, n_features=n_features, n_informative=n_informative)
  pdf = pd.DataFrame(x, dtype=object).rename(lambda x: 'c'+str(x), axis='columns')
  cols = list(pdf.columns)
  pdf['label'] = y
  pdf['index'] = pdf.index
  reordered = ['index','label',*cols]
  #print(reordered)
  #return pdf[reordered]
  return spark.createDataFrame(pdf[reordered])

make_regression(n_samples=1_000_000).display()

# COMMAND ----------

from pyspark.sql import DataFrame
import sklearn
import pandas as pd

def make_classification(n_samples=1000, n_features=10, n_classes=4) -> DataFrame:
  """
    create a dense data set usable in regression modeling and load testing.
    The dataset will have an unique integer identifier column, a label column and floating point feature columns.
    This generator is based on the sklearn make_regression
  """
  x,y = sklearn.datasets.make_multilabel_classification(n_samples=n_samples, n_features=n_features, n_classes=n_classes)
  label_pdf = pd.DataFrame(y).rename(lambda x: 'label'+str(x), axis='columns')
  index_pdf = pd.DataFrame(label_pdf.index).rename(lambda x: 'index', axis='columns')
  features_pdf = pd.DataFrame(x, dtype=object).rename(lambda x: 'c'+str(x), axis='columns')
  pdf = pd.concat([index_pdf, label_pdf, features_pdf],axis=1)
  return spark.createDataFrame(pdf)

make_classification().display()

# COMMAND ----------

n_features = 10

x,y = sklearn.datasets.make_multilabel_classification(n_samples=390, n_features=n_features, n_classes=4)
label_pdf = pd.DataFrame(y).rename(lambda x: 'label'+str(x), axis='columns')
index_pdf = pd.DataFrame(label_pdf.index).rename(lambda x: 'index', axis='columns')
features_pdf = pd.DataFrame(x, dtype=object).rename(lambda x: 'c'+str(x), axis='columns')
pdf = pd.concat([index_pdf, label_pdf, features_pdf],axis=1)
pdf

# COMMAND ----------


df_new = pd.concat([pd.DataFrame(y).rename(lambda x: 'label'+str(x), axis='columns'), pdf], axis=1)
df_new

# COMMAND ----------


