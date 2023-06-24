# Databricks notebook source
# MAGIC %md # Bokeh based plotting
# MAGIC ![Bokeh](https://static.bokeh.org/logos/logotype.svg)
# MAGIC Bokeh is an interactive visualization library for modern web browsers. It provides elegant, concise construction of versatile graphics, and affords high-performance interactivity over large or streaming datasets. Bokeh can help anyone who would like to quickly and easily make interactive plots, dashboards, and data applications.
# MAGIC Bokeh is a powerful package on it's own and a powerful backend for many other packages (e.g. hvplot, pandas)

# COMMAND ----------

# in ML 5.3 ML cluster, need to explicitly import bokeh

from bokeh.plotting import figure
from bokeh.embed import components, file_html
from bokeh.resources import CDN

# prepare some data
x = [1, 2, 3, 4, 5]
y = [6, 7, 2, 4, 5]

# create a new plot with a title and axis labels
p = figure(title="simple line example", x_axis_label='x', y_axis_label='y')

# add a line renderer with legend and line thickness
p.line(x, y, legend_label="Temp.", line_width=2)

# create an html document that embeds the Bokeh plot
html = file_html(p, CDN, "my plot1")

# display this html
displayHTML(html)

# COMMAND ----------


