# Databricks notebook source
# MAGIC %md ## <div><b style="font-size: 160%; color: #1CA0C2;">&#8678;</b> <a href="$./2_Ggplot_Viz_Examples">Ggplot Viz Examples</a> <div style="float:right"><a href="$./4_Plotly_Viz_Examples">Plotly Viz Examples</a> <b style="font-size: 160%; color: #1CA0C2;">&#8680;</b></div></div>

# COMMAND ----------

# MAGIC %md # Bokeh Vizualization Examples in Databricks
# MAGIC
# MAGIC > Bokeh is a Python interactive visualization library that targets modern web browsers for presentation. Its goal is to provide elegant, concise construction of novel graphics in the style of D3.js, and to extend this capability with high-performance interactivity over very large or streaming datasets. ([bokeh.pydata.org](https://bokeh.pydata.org/en/latest/))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC <!-- NOTE HOW MARKDOWN AND HTML CAN BE MIXED -->
# MAGIC
# MAGIC <div style="width: 1100px;">
# MAGIC <img src="http://www.freddiemac.com/images/FreddieMacLogo.svg" width="20%" style="float: left; margin-right: 30px;">
# MAGIC __Explore Single Family Loan-Level Dataset__
# MAGIC   <div style="margin: 0 auto; width: 600px; height: 50px;"> <!-- use `background: green;` for testing -->
# MAGIC     <ul>
# MAGIC       <li>[Dataset Overview](http://www.freddiemac.com/research/datasets/sf_loanlevel_dataset.html)</li>
# MAGIC       <li>[Users Guide](http://www.freddiemac.com/research/pdf/user_guide.pdf)</li>
# MAGIC     </ul>
# MAGIC   </div>
# MAGIC </div>  
# MAGIC _The dataset covers approximately 24.5 million fixed-rate mortgages (including HARP loans) originated between January 1, 1999 and June 30, 2016.  Monthly loan performance data, including credit performance information up to and including property disposition, is being disclosed through December 31, 2016._

# COMMAND ----------

# MAGIC %md ### Setup 
# MAGIC
# MAGIC __Verify bokeh python library is installed. Options are via API, Library UI, or `pip`.__

# COMMAND ----------

# MAGIC %sh /databricks/python/bin/pip list --format=columns | grep bokeh

# COMMAND ----------

# MAGIC %md ### Common Imports
# MAGIC
# MAGIC _Using the embedded support from bokeh, then display with databricks support for `displayHTML()`._

# COMMAND ----------

from bokeh.embed import components,  file_html # notebook_div,
from bokeh.layouts import row, column
from bokeh.models import BoxSelectTool, LassoSelectTool, Spacer
from bokeh.plotting import figure, curdoc
from bokeh.resources import CDN

import numpy as np
import json

# COMMAND ----------

# MAGIC %md ### Common Functions

# COMMAND ----------

# MAGIC %run ./Helper_Functions

# COMMAND ----------

def clearCurDoc():
  """
  Run this at the top of each new plot for rendering to clear `curdoc`.
  """
  curdoc().clear()
  
def displayCurDoc(title="bokeh plot"):
  """
  Display `curdoc` in Databricks.
  """
  # create an html document that embeds the Bokeh plot and display it
  displayHTML(file_html(curdoc(), CDN, title))

# COMMAND ----------

# MAGIC %md ### Prepare Data
# MAGIC
# MAGIC __DataFrame from SQL Table/View__

# COMMAND ----------

from bokeh.models import HoverTool
from bokeh.models.formatters import NumeralTickFormatter, PrintfTickFormatter

x= jsonColList(jlist, 'ltv', 0)
y= jsonColList(jlist, 'avg_unpaid', 0)

clearCurDoc() # helper function to reset curdoc

hover = HoverTool(tooltips=[
    ("LTV", "@x{int}%"),  
    ("AVG", "@y{$0,0.00}")
])

# create a new plot with a title and axis labels
p = figure(
  title="LTV vs Unpaid_Balance",
  x_axis_label='LTV', y_axis_label='Unpaid_Balance', 
  plot_width=800, plot_height=600,
)

p.left[0].formatter = NumeralTickFormatter(format="$0,0.00")
p.below[0].formatter = PrintfTickFormatter(format="%i%%")

p.add_tools(hover) # add tooltips to the defaults

# turn off scientific notation (not needed with numeral tick formatter)
# p.left[0].formatter.use_scientific = False

# add a line renderer with legend and line thickness
p.line(x, y) #legend="Series", line_width=2
p.circle(x, y, color="navy", alpha=0.5)

# create an html document that embeds the Bokeh plot
html = file_html(p, CDN, "my plot1")

# display this html
displayHTML(html)

# COMMAND ----------

# MAGIC %md ### Property Type vs Property State: Matrix Example (`toPandas()`)
# MAGIC
# MAGIC _Top `property_type` loans are to Single Family (SF), Planned Unit Development (PU), and Condo (CO)._

# COMMAND ----------

# MAGIC %md __Further Prep the Data__

# COMMAND ----------

states = ([ str(x) for x in
jsonColList(
  dfToJson(
    ldf
    .select("property_state")
    .distinct()
    .dropna()
    .orderBy("property_state")
  ),
  'property_state', 
  ''
  )
])

states[:5]

# COMMAND ----------

property_types = ([ str(x) for x in
jsonColList(
  dfToJson(
    ldf
    .select("property_type")
    .distinct()
    .dropna()
    .orderBy("property_type")
  ),
  'property_type', 
  ''
  )
])

property_types

# COMMAND ----------

# property_type vs property_state
matrix_df = (
ldf.select("property_type","property_state")
  .dropna()
  .groupBy("property_type","property_state")
  .count()
  .orderBy(["property_state","property_type"])
).selectExpr("property_state","property_type","CAST(count AS int) as type_count")
display(matrix_df)

# COMMAND ----------

# MAGIC %md __Render__

# COMMAND ----------

""" Adapted from https://bokeh.pydata.org/en/latest/docs/gallery/unemployment.html"""
from math import pi
import pandas as pd
import seaborn as sns

# from bokeh.io import show
from bokeh.models import (
    ColumnDataSource,
    HoverTool,
    LinearColorMapper,
    BasicTicker,
    PrintfTickFormatter,
    ColorBar,
)
from bokeh.plotting import figure
# from bokeh.sampledata.unemployment1948 import data

clearCurDoc() # helper function to reset curdoc

pandas_matrix = matrix_df.toPandas() # spark df to pandas df
pandas_matrix['property_state'] = pandas_matrix['property_state'].astype(str)
pandas_matrix['property_type'] = pandas_matrix['property_type'].astype(str)
df = pandas_matrix.set_index(['property_state','property_type','type_count']).reset_index()


# colors = ["#75968f", "#a5bab7", "#c9d9d3", "#e2e2e2", "#dfccce", "#ddb7b1", "#cc7878", "#933b41", "#550b1d"] # this is the colormap from the original NYTimes plot
colors = sns.color_palette("Paired").as_hex() # using seaborn palette
mapper = LinearColorMapper(palette=colors, low=df.type_count.min(), high=df.type_count.max())

source = ColumnDataSource(df)

TOOLS = "hover,save,pan,box_zoom,reset,wheel_zoom"

p = figure(title="State - Property Type Matrix",
           x_range=states, 
           y_range=list(reversed(property_types)),
           x_axis_location="above", 
           plot_width=1000, 
           plot_height=300,
           tools=TOOLS, 
           toolbar_location='below')

p.grid.grid_line_color = None
p.axis.axis_line_color = None
p.axis.major_tick_line_color = None
p.axis.major_label_text_font_size = "10pt"
p.axis.major_label_standoff = 0
p.xaxis.major_label_orientation = pi / 3

p.rect(x="property_state", y="property_type", width=1, height=1,
       source=source,
       fill_color={'field': 'type_count', 'transform': mapper},
       line_color=None)


color_bar = ColorBar(color_mapper=mapper, major_label_text_font_size="6pt",
                     ticker=BasicTicker(desired_num_ticks=len(colors)),
                     formatter=NumeralTickFormatter(format="0,0"),
                     label_standoff=6, border_line_color=None, location=(0, 0))
p.add_layout(color_bar, 'right')

p.select_one(HoverTool).tooltips = [
     ('State', '@property_state'),
     ('Type', '@property_type'),
     ('Count', '@type_count{0,0}'),
]

curdoc().add_root(p)
curdoc().title = "State - Property Type Matrix"

displayCurDoc() # helper function for databricks display

# COMMAND ----------

# MAGIC %md ### Example Histogram with Linked Components
# MAGIC
# MAGIC _This is not backed by financial data, just an example of more complex visualization._

# COMMAND ----------

"""
Present a scatter plot with linked histograms on both axes.
 * adapted from https://github.com/bokeh/bokeh/blob/master/examples/app/selection_histogram.py
 * MJ: Adding call to explicitely clear contents on the current document (otherwise it will append)
"""
clearCurDoc() # helper function to reset curdoc

# create three normal population samples with different parameters
x1 = np.random.normal(loc=5.0, size=400) * 100
y1 = np.random.normal(loc=10.0, size=400) * 10

x2 = np.random.normal(loc=5.0, size=800) * 50
y2 = np.random.normal(loc=5.0, size=800) * 10

x3 = np.random.normal(loc=55.0, size=200) * 10
y3 = np.random.normal(loc=4.0, size=200) * 10

x = np.concatenate((x1, x2, x3))
y = np.concatenate((y1, y2, y3))

TOOLS="pan,wheel_zoom,box_select,lasso_select,reset"

# create the scatter plot
p = figure(tools=TOOLS, plot_width=600, plot_height=600, min_border=10, min_border_left=50,
           toolbar_location="above", x_axis_location=None, y_axis_location=None,
           title="Linked Histograms")
p.background_fill_color = "#fafafa"
p.select(BoxSelectTool).select_every_mousemove = False
p.select(LassoSelectTool).select_every_mousemove = False

r = p.scatter(x, y, size=3, color="#3A5785", alpha=0.6)

# create the horizontal histogram
hhist, hedges = np.histogram(x, bins=20)
hzeros = np.zeros(len(hedges)-1)
hmax = max(hhist)*1.1

LINE_ARGS = dict(color="#3A5785", line_color=None)

ph = figure(toolbar_location=None, plot_width=p.plot_width, plot_height=200, x_range=p.x_range,
            y_range=(-hmax, hmax), min_border=10, min_border_left=50, y_axis_location="right")
ph.xgrid.grid_line_color = None
ph.yaxis.major_label_orientation = np.pi/4
ph.background_fill_color = "#fafafa"

ph.quad(bottom=0, left=hedges[:-1], right=hedges[1:], top=hhist, color="white", line_color="#3A5785")
hh1 = ph.quad(bottom=0, left=hedges[:-1], right=hedges[1:], top=hzeros, alpha=0.5, **LINE_ARGS)
hh2 = ph.quad(bottom=0, left=hedges[:-1], right=hedges[1:], top=hzeros, alpha=0.1, **LINE_ARGS)

# create the vertical histogram
vhist, vedges = np.histogram(y, bins=20)
vzeros = np.zeros(len(vedges)-1)
vmax = max(vhist)*1.1

pv = figure(toolbar_location=None, plot_width=200, plot_height=p.plot_height, x_range=(-vmax, vmax),
            y_range=p.y_range, min_border=10, y_axis_location="right")
pv.ygrid.grid_line_color = None
pv.xaxis.major_label_orientation = np.pi/4
pv.background_fill_color = "#fafafa"

pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vhist, color="white", line_color="#3A5785")
vh1 = pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vzeros, alpha=0.5, **LINE_ARGS)
vh2 = pv.quad(left=0, bottom=vedges[:-1], top=vedges[1:], right=vzeros, alpha=0.1, **LINE_ARGS)

layout = column(row(p, pv), row(ph, Spacer(width=200, height=200)))

curdoc().add_root(layout)
curdoc().title = "Selection Histogram"

def update(attr, old, new):
    inds = np.array(new['1d']['indices'])
    if len(inds) == 0 or len(inds) == len(x):
        hhist1, hhist2 = hzeros, hzeros
        vhist1, vhist2 = vzeros, vzeros
    else:
        neg_inds = np.ones_like(x, dtype=np.bool)
        neg_inds[inds] = False
        hhist1, _ = np.histogram(x[inds], bins=hedges)
        vhist1, _ = np.histogram(y[inds], bins=vedges)
        hhist2, _ = np.histogram(x[neg_inds], bins=hedges)
        vhist2, _ = np.histogram(y[neg_inds], bins=vedges)

    hh1.data_source.data["top"]   =  hhist1
    hh2.data_source.data["top"]   = -hhist2
    vh1.data_source.data["right"] =  vhist1
    vh2.data_source.data["right"] = -vhist2

r.data_source.on_change('selected', update)

displayCurDoc() # helper function for databricks display

# COMMAND ----------

# MAGIC %md ## <div><b style="font-size: 160%; color: #1CA0C2;">&#8678;</b> <a href="$./2_Ggplot_Viz_Examples">Ggplot Viz Examples</a> <div style="float:right"><a href="$./4_Plotly_Viz_Examples">Plotly Viz Examples</a> <b style="font-size: 160%; color: #1CA0C2;">&#8680;</b></div></div>
