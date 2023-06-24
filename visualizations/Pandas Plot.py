# Databricks notebook source
# MAGIC %md # Plotting with Pandas
# MAGIC * Example with default Pandas backend
# MAGIC * Example with Holoviews & hvplot charting backend
# MAGIC ## Requirements
# MAGIC * DBR 5.x, 6.x ML Runtime
# MAGIC * Python Packages
# MAGIC   * bokeh
# MAGIC   * holoviews
# MAGIC   * hvplot

# COMMAND ----------

# DBTITLE 1,Generate random walk Pandas data frame
import numpy as np
import pandas as pd
print(pd.__version__)

index = pd.date_range('1/1/2000', periods=1000)
df = pd.DataFrame(np.random.randn(1000, 4), index=index, columns=list('ABCD')).cumsum()

df.head()

# COMMAND ----------

# DBTITLE 1,Plot with Pandas data frame default Matplotlib backend
#%matplotlib inline

# defaults to matplotlib backend
df.plot();

display()  # Databricks specific

# COMMAND ----------

# MAGIC %md # Plot with Holoviews, Bokeh & hvplot

# COMMAND ----------

import math
import numpy as np
import pandas as pd
import holoviews as hv
#import networkx as nx
#from holoviews.element.graphs import layout_nodes

from bokeh.embed import components, file_html
from bokeh.resources import CDN

hv.extension('bokeh')
renderer = hv.renderer('bokeh').instance(fig='html', holomap='auto')

# see https://github.com/ioam/holoviews/issues/1819
def displayHoloviews(hv_plot, html_name="plot.html", width=1000, height=600, renderer=renderer):
  plot = renderer.get_plot(hv_plot).state
  setattr(plot, 'plot_width', width)
  setattr(plot, 'plot_height', height)
  displayHTML(file_html(plot, CDN, html_name))

import hvplot

# COMMAND ----------

displayHoloviews(hvplot.hvPlot(df).line(y=['A','B','C','D']))
