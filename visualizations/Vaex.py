# Databricks notebook source
# MAGIC %md # Vaex
# MAGIC Renders large datasets (millions and billions of points) very quickly.
# MAGIC - https://vaex.io/docs/index.html
# MAGIC

# COMMAND ----------

# MAGIC %pip install vaex

# COMMAND ----------

import warnings; warnings.filterwarnings("ignore")
import vaex
df = vaex.open('s3://vaex/taxi/yellow_taxi_2015_f32s.hdf5?anon=true')

long_min = -74.05
long_max = -73.75
lat_min = 40.58
lat_max = 40.90

df.plot(df.pickup_longitude, df.pickup_latitude, f="log1p", limits=[[-74.05, -73.75], [40.58, 40.90]], show=True);

# COMMAND ----------

df.close_files()

# COMMAND ----------


