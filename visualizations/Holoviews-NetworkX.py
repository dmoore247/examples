# Databricks notebook source
# MAGIC %md # Holoviews and NetworkX

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

import networkx as nx
G = nx.karate_club_graph()
padding = dict(x=(-1.2, 1.2), y=(-1.2, 1.2))
simple_graph = hv.Graph.from_networkx(G, nx.layout.circular_layout).redim.range(**padding)
displayHoloviews(simple_graph)
