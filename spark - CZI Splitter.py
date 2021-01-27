# Databricks notebook source
# DBTITLE 1,Pandas Image Splitter UDF mockup
from pyspark.ml.image import ImageSchema
from pyspark.sql.types import LongType, StringType, StructType, StructField, IntegerType, BinaryType
from pyspark.sql.functions import expr

from typing import Iterator
import pandas as pd
import numpy as np

def pandas_czi_splitter(paths: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in paths:
      path = pdf['path'][0]
      path = path[0:-4]
      for i in range(3): # Slices
        for j in range(5): # Xtiles
          for k in range(7):  # Ytiles
            pdf['path'] = path
            pdf['slice'] = i
            pdf['xtile'] = j
            pdf['ytile'] = k
            pdf['origin'] = F"{path}_slice{i}_xtile{j}_ytile{k}.png"
            pdf['height'] = 128
            pdf['width']  = 128
            pdf['nChannels'] = 4
            pdf['mode'] = 24
            pdf['data'] = np.asarray(b'this is binary data')
        
        yield pdf

schema = StructType([
          StructField('path',StringType(),True),    # input
          StructField('slice',IntegerType(),True),  # intermediate
          StructField('xtile',IntegerType(),True),  # intermediate
          StructField('ytile',IntegerType(),True),  # intermediate
          StructField('origin',StringType(),True),  # output
          StructField('height',IntegerType(),False),
          StructField('width',IntegerType(),False),
          StructField('nChannels',IntegerType(),False),
          StructField('mode',IntegerType(),False),
          StructField('data',BinaryType(),False)])

# Integration test
df = spark.range(4, numPartitions=10).withColumn('path',expr("concat('s3a://this/is/my/object/path_czi', string(id), '.czi')"))

#Dataframe.mapInPandas returns zero, one or more rows for every input

dfx = df.mapInPandas(pandas_czi_splitter, schema=schema)
dfx.count()
display(dfx)