# Databricks notebook source
import pandas as pd
import numpy as np
import holoviews as hv
from holoviews import opts
hv.extension('matplotlib')

# COMMAND ----------

normal = np.random.randn(1000, 2)
hv.Bivariate(normal)
