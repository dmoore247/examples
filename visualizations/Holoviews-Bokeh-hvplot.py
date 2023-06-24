# Databricks notebook source
# MAGIC %md # Plotting Pandas with Holoviews, Bokeh, hvplot
# MAGIC * Holoviews charting with backend Bokeh backend
# MAGIC ## Requirements
# MAGIC * DBR 5.x, 6.x ML Runtime
# MAGIC * Python Packages
# MAGIC   * bokeh
# MAGIC   * holoviews
# MAGIC   * hvplot

# COMMAND ----------

# DBTITLE 1,Imports and Setup
import math
import numpy as np
import pandas as pd
import holoviews as hv
from bokeh.embed import components, file_html
from bokeh.resources import CDN

hv.extension('bokeh')

import hvplot

renderer = hv.renderer('bokeh').instance(fig='html', holomap='auto')

# see https://github.com/ioam/holoviews/issues/1819
def displayHoloviews(hv_plot, html_name="plot.html", width=1000, height=600, renderer=renderer):
  plot = renderer.get_plot(hv_plot).state
  setattr(plot, 'plot_width', width)
  setattr(plot, 'plot_height', height)
  displayHTML(file_html(plot, CDN, html_name))

# COMMAND ----------

# DBTITLE 1,Generate random walk Pandas data frame
index = pd.date_range('1/1/2000', periods=1000)
df = pd.DataFrame(np.random.randn(1000, 4), index=index, columns=list('ABCD')).cumsum()

df.head()

# COMMAND ----------

displayHoloviews(hvplot.hvPlot(df).line(y=['A','B','C','D']))
