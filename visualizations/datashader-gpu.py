# Databricks notebook source
# MAGIC %md 
# MAGIC Datashader accelerated by GPU 
# MAGIC https://github.com/holoviz/datashader/pull/793

# COMMAND ----------

import time
import os
import numpy as np
import pandas as pd
#from cudf import cudf
#import cudf_cuda92
from numba import cuda as nb_cuda
import datashader as ds
from datashader import transfer_functions as tf
from datashader.colors import Hot
import dask.dataframe as dd

pdf = pd.read_csv('/media/jmmease/ExtraDrive1/examples/datashader-examples/data/nyc_taxi.csv',
                  usecols=['dropoff_x','dropoff_y', 'passenger_count', 'payment_type']
                 ) # Load into Pandas DF and select rows

pdf['passenger_count'] = pdf['passenger_count'].astype(np.float64) # Convert aggregation column
pdf['passenger_cat'] = pdf['passenger_count'].astype('int8').astype('category')
pdf['payment_type'] = pdf['payment_type'].astype('int8').astype('category')

# Replicate DataFrame to increase size
reps = 10
pdf = pd.concat([pdf]*reps)
pdf.head()

# COMMAND ----------


