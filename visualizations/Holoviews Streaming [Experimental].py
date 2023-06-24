# Databricks notebook source
dbutils.library.installPyPI("holoviews")

# COMMAND ----------

# MAGIC %sh /databricks/conda/bin/pip install holoviews

# COMMAND ----------

# MAGIC %sh /databricks/conda/bin/pip install https://github.com/python-streamz/streamz

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import streamz

# COMMAND ----------

import time
import numpy as np
import pandas as pd
import holoviews as hv
#import streamz
#import streamz.dataframe

from holoviews import opts
from holoviews.streams import Pipe, Buffer

hv.extension('bokeh')

# COMMAND ----------

from bokeh.embed import components, file_html
from bokeh.resources import CDN

renderer = hv.renderer('bokeh').instance(fig='html', holomap='auto')

# see https://github.com/ioam/holoviews/issues/1819
def displayHoloviews(hv_plot, html_name="plot.html", width=1000, height=600, renderer=renderer):
  plot = renderer.get_plot(hv_plot).state
  setattr(plot, 'plot_width', width)
  setattr(plot, 'plot_height', height)
  displayHTML(file_html(plot, CDN, html_name))

# COMMAND ----------

pipe = Pipe(data=[])
vector_dmap = hv.DynamicMap(hv.VectorField, streams=[pipe])
x = vector_dmap.opts(color='Magnitude', xlim=(-1, 1), ylim=(-1, 1))
print(x)
#displayHoloviews(x)

# COMMAND ----------

import IPython
IPython.__version__

# COMMAND ----------

from bokeh.io import push_notebook, show, output_notebook
from bokeh.plotting import figure
output_notebook() # <- fails

# COMMAND ----------

help(output_notebook())

# COMMAND ----------

print(output_notebook().__repr__())

# COMMAND ----------

from bokeh.plotting import figure 
from bokeh.io import output_notebook, show
output_notebook()
from numpy import cos, linspace
x = linspace(-6, 6, 100)
y = cos(x)

p = figure(width=500, height=500)
p.circle(x, y, size=7, color="firebrick", alpha=0.5)
show(p)

# COMMAND ----------

displayHTML(show(p))

# COMMAND ----------


