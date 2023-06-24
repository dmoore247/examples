# Databricks notebook source
# MAGIC %md # Plotnine, A ggplot inspired Python package
# MAGIC
# MAGIC In Databricks Runtime 6.0 and above, you cannot call the display function on Python ggplot objects because the ggplot package is not compatible with newer version of pandas. Plotnine is the answer to missing ggplot package
# MAGIC
# MAGIC ## Requirements
# MAGIC * plotnine
# MAGIC
# MAGIC *reference: https://docs.databricks.com/notebooks/visualizations/matplotlib-and-ggplot.html*

# COMMAND ----------

from plotnine import *
from plotnine.data import meat
from mizani.breaks import date_breaks
from mizani.formatters import date_format

pn = ggplot(meat, aes('date','beef')) + \
    geom_line(color='black') + \
    scale_x_date(breaks=date_breaks('7 years'), labels=date_format('%b %Y')) + \
    scale_y_continuous() + theme_bw() + theme(figure_size=(12, 8))

# COMMAND ----------

display(display(pn.draw()))

# COMMAND ----------


