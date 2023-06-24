# Databricks notebook source
dbutils.library.installPyPI('altair', '4.0.0')  # Data Visualization library that uses XDR/JSON to fetch out of band data

# COMMAND ----------

import pandas as pd
import numpy as np
import altair as alt
#alt.renderers.enable('html')
n = 50000 # works
#n = 500000 # fails, result too large
alt.data_transformers.enable(max_rows=n)
df = pd.DataFrame({
    'x': np.arange(n),
    'y': np.random.randn(n),
})

big_chart = alt.Chart(df).mark_line().encode(
    x='x',
    y='y'
)

displayHTML(big_chart.to_html())

# COMMAND ----------


