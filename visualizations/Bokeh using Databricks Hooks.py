# Databricks notebook source
# MAGIC %md # Bokeh plot using a `databricks` hook
# MAGIC
# MAGIC Plot Bokeh plot in Databricks notebook by creating a Databricks Hook
# MAGIC
# MAGIC Process:
# MAGIC 1. Setup Callbacks, using `displayHTML()` in `show_doc`
# MAGIC 2. Install Callbacks (Hooks)
# MAGIC 3. Configure Bokeh to use Databricks hooks `output_notebook(notebook_type='databricks')`

# COMMAND ----------

import sys

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

# COMMAND ----------

# DBTITLE 1,Build Databricks backend for Bokeh
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.embed import file_html

from bokeh.embed.notebook import notebook_content
from bokeh.io.notebook import publish_display_data
from bokeh.util.serialization import make_id

def databricks_show_doc(
  obj,            # the Bokeh object to display
  state,          # current bokeh.io "state"
  notebook_handle # whether a notebook handle was requested
  ):
  displayHTML(file_html(obj, CDN, "my plot1"))
  return None

# Following https://github.com/bokeh/bokeh/blob/master/bokeh/io/notebook.py
debug = False
def databricks_load(
      resources,   # A Resources object for how to load BokehJS
      verbose,     # Whether to display verbose loading banner
      hide_banner, # Whether to hide the output banner entirely
      load_timeout # Time after which to report a load fail error
  ):
  if(debug):
    print('load ' + str(resources) + '  ' + str(verbose) + '  ' + str(hide_banner) + '  ' + str(load_timeout) )

def databricks_show_app(
  app,          # the Bokeh Application to display
  state,        # current bokeh.io "state"
  notebook_url, # URL to the current active notebook page
  **kw          # any backend-specific keywords passed as-is
  ):
  print('show app')

from bokeh.io import install_notebook_hook
install_notebook_hook(notebook_type='databricks', load=databricks_load, show_doc=databricks_show_doc, show_app=databricks_show_app, overwrite=True)

# COMMAND ----------

# MAGIC %md ## Bokeh Example
# MAGIC
# MAGIC See: https://github.com/bokeh/bokeh/blob/2.0.2/examples/howto/notebook_comms/Continuous%20Updating.ipynb

# COMMAND ----------

import time
import numpy as np
from bokeh.io import show, output_notebook
from bokeh.models import HoverTool
from bokeh.plotting import figure

output_notebook(notebook_type='databricks') # Set the hook

# COMMAND ----------

# DBTITLE 1,Generate the data (N random circles)
N = 100000
x = np.random.random(size=N) * 100
y = np.random.random(size=N) * 100
radii = np.random.random(size=N) * 2
colors = ["#%02x%02x%02x" % (int(r), int(g), 150) for r, g in zip(50+2*x, 30+2*y)]

# COMMAND ----------

# DBTITLE 1,Define the visualization
TOOLS="crosshair,pan,wheel_zoom,box_zoom,reset,tap,box_select,lasso_select"

p = figure(tools=TOOLS)
p.axis.major_label_text_font_size = "24px"
hover = HoverTool(tooltips=None, mode="vline")
p.add_tools(hover)
r = p.circle(x,y, radius=radii, 
             fill_color=colors, fill_alpha=0.6, line_color=None, 
             hover_fill_color="black", hover_fill_alpha=0.7, hover_line_color=None)

# COMMAND ----------

help(show)

# COMMAND ----------

# DBTITLE 1,Render Bokeh Plot using native show()
show(p)

# COMMAND ----------


