# Databricks notebook source
# MAGIC %md # Data Generator: make_classification
# MAGIC Create a dense data set  usable in classification modeling and load testing.
# MAGIC The dataset will have an unique integer identifier column, a label column and floating point feature columns.
# MAGIC This generator is based on the sklearn make_multilabel_classification
# MAGIC 
# MAGIC @returns: Spark DataFrame
# MAGIC 
# MAGIC @author: Douglas Moore
# MAGIC 
# MAGIC @tags: data generator, sklearn, ml, spark, dataframe

# COMMAND ----------

from pyspark.sql import DataFrame
import sklearn
import pandas as pd

def make_classification(n_samples=1000, n_features=10, n_classes=4) -> DataFrame:
  """
    Create a dense data set usable in classification modeling and load testing.
    The dataset will have an unique integer identifier column, a label column and floating point feature columns.
    This generator is based on the sklearn make_multilabel_classification
  """
  x,y = sklearn.datasets.make_multilabel_classification(n_samples=n_samples, n_features=n_features, n_classes=n_classes)
  label_pdf = pd.DataFrame(y).rename(lambda x: 'label'+str(x), axis='columns')
  index_pdf = pd.DataFrame(label_pdf.index).rename(lambda x: 'index', axis='columns')
  features_pdf = pd.DataFrame(x, dtype=object).rename(lambda x: 'c'+str(x), axis='columns')
  pdf = pd.concat([index_pdf, label_pdf, features_pdf],axis=1)
  return spark.createDataFrame(pdf)

make_classification().display()

# COMMAND ----------


