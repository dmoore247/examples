# Databricks notebook source
# MAGIC %md ### Setup
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 0. matplotlib
# MAGIC 0. ipykernel==5.1.2 [For `%matplotlib inline`]
# MAGIC 0. DBR 6.3  [For `%matplotlib inline`]
# MAGIC
# MAGIC Spark settings: 
# MAGIC ```
# MAGIC spark.databricks.conda.condaMagic.enabled true
# MAGIC spark.databricks.workspace.matplotlibInline.enabled true
# MAGIC ```

# COMMAND ----------

# MAGIC %conda install bokeh==1.4.0 hvplot pandas==1.* holoviews hvplot

# COMMAND ----------

# MAGIC %conda install -c plotly plotly_express==0.4.0 plotly==4.8.1

# COMMAND ----------

# MAGIC %md #### Matplotilb 
# MAGIC Release currently targeted for DBR 6.3 <a href="https://logfood.cloud.databricks.com/#notebook/10088563/command/10088564">[See AWS Logfood for working example]</a>
# MAGIC
# MAGIC ## Requires
# MAGIC * DBR 6.3
# MAGIC * Spark cluster configuration flag: `spark.databricks.workspace.matplotlibInline.enabled true`
# MAGIC * Python package `ipykernel==5.1.2`
# MAGIC * matplotlib

# COMMAND ----------

# MAGIC %md #### Matplotlib and synthetic data setup

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

import numpy as np
import matplotlib.pyplot as plt
plt.style.use('seaborn-dark-palette')

# setup synthetic chart
points, zorder1, zorder2 = 500, 10, 5
x = np.linspace(0, 1, points)
y = np.sin(zorder1 * np.pi * x) * np.exp(-zorder2 * x)

# COMMAND ----------

# MAGIC %md # Portable Data Visualization Strategies

# COMMAND ----------

# MAGIC %md
# MAGIC 0. Hope, Magic & Hints
# MAGIC 0. Image Buffer
# MAGIC 0. Image file
# MAGIC 0. Render to HTML
# MAGIC   * Matplotlib based animations
# MAGIC 0. Data Lake to Visualization

# COMMAND ----------

# MAGIC %md ## Strategy: Hope, Magic and Hints

# COMMAND ----------

# DBTITLE 1,Leverage Matplotlib backend and Databricks display()
fig, ax = plt.subplots()

ax.fill(x, y, zorder=zorder1)
ax.grid(True, zorder=zorder2)
plt.show()
display() # Databricks display
plt.close() # Ensure you close it

# COMMAND ----------

# MAGIC %md ### %matplotlib inline
# MAGIC Will inline matplotlib visuals without needing the display() call.

# COMMAND ----------

# DBTITLE 0,Use %matplotlib inline
# MAGIC %matplotlib inline

# COMMAND ----------

fig, ax = plt.subplots()

ax.fill(x, y, zorder=zorder1)
ax.grid(True, zorder=zorder2)
plt.show()
plt.close() # Ensure you close it

# COMMAND ----------

# MAGIC %md ## Strategy: Render as an image to an in-memory buffer
# MAGIC * Create BytesIO buffer
# MAGIC * Save figure to buffer
# MAGIC * encode buffer as base64, embed in HTML, display

# COMMAND ----------

# DBTITLE 0,Matplotlib figure embedded into HTML
import base64
from io import BytesIO
tmpfile = BytesIO()

fig, ax = plt.subplots()
ax.fill(x, y, zorder=zorder1)
ax.grid(True, zorder=zorder2)

title = "Save figure to in-memory buffer as a PNG image"
fig.savefig(tmpfile, format='png') 

encoded = base64.b64encode(tmpfile.getvalue()).decode("UTF-8")
plt.close() # make sure we don't display figure directly due to %matplotlib inlinee
displayHTML(F"<p><b>Title: {title}</b></p><img src=\'data:image/png;base64,{encoded}\'>")

# COMMAND ----------

# MAGIC %md ## Strategy: Render to an Image file
# MAGIC * Graphics package doesn't support BytesIO / StringIO
# MAGIC * Need to store plot for audit purposes
# MAGIC * Embed in Markdown or HTML

# COMMAND ----------

# MAGIC %md ### Log Matplotlib figure to tracking API MLFlow

# COMMAND ----------

# DBTITLE 1,Log Matplotlib figure to MLFlow
import mlflow

with mlflow.start_run():
  fig, ax = plt.subplots()
  ax.fill(x, y, zorder=zorder1)
  ax.grid(True, zorder=zorder2)
  file_path = "/dbfs/FileStore/plots/sine-display.png"
  fig.savefig(file_path, format='png')
  mlflow.log_params({ 
    "points": points,
    "zorder1": zorder1,
    "zorder2": zorder2}) # parameters
  mlflow.log_artifact(file_path) # image

plt.close('all') # note, no display below

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integrate saved images with Markdown
# MAGIC `![Sine Display](files/plots/sine-display.png)`|
# MAGIC
# MAGIC ![Sine Display](files/plots/sine-display.png)

# COMMAND ----------

# MAGIC %md ## Strategy: HTML/JavaScript/JSON data in-line
# MAGIC * Pandas DataFrame.plot() with Bokeh backend
# MAGIC * Generates HTML/JavaScript/JSON data 
# MAGIC * Bokeh compacts data with __ndarray__ binary data type
# MAGIC * Displays HTML

# COMMAND ----------

# DBTITLE 1,Pandas Bokeh plot
import pandas as pd
import numpy as np

num = 95000
ts = pd.Series(np.random.randn(num), index=pd.date_range('1/1/1', periods=num)).cumsum()

## Bokeh plot setup, requires pandas > 0.25 and pandas_bokeh install
pd.set_option('plotting.backend', 'pandas_bokeh')

# Needed to render HTML
from bokeh.embed import components, file_html
from bokeh.resources import CDN

# Databricks displayHTML, Bokeh's file_html, and CDN
displayHTML(file_html(ts.plot_bokeh(), CDN, "my plot1"))

# COMMAND ----------

import pandas as pd
import numpy as np
import plotly
import plotly.express as px

pd.options.plotting.backend = "plotly"
num = 1000
ts = pd.Series(np.random.randn(num), index=pd.date_range('1/1/2000', periods=num)).cumsum()

ts.plot()

# COMMAND ----------

# MAGIC %md ### Matplotlib based animations
# MAGIC * need to use displayHTML()
# MAGIC * need to convert to jshtml
# MAGIC * If `%matplotlib inline` has already be run, the `ax.plot` command may interfere. Clear your notebook, attach/detatch to undo `%matplotlib inline` and re-run command below
# MAGIC * Putting displayHTML() into separate command block will bypass issues from above.
# MAGIC
# MAGIC Successful run will produce a centered window with video run commands

# COMMAND ----------

# MAGIC %matplotlib inline

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.animation
import numpy as np
plt.ioff()
t = np.linspace(0,2*np.pi)
x = np.sin(t)
fig, ax = plt.subplots()
l, = ax.plot([0,2*np.pi],[-1,1])
animate = lambda i: l.set_data(t[:i], x[:i])
ani = matplotlib.animation.FuncAnimation(fig, animate, frames=len(t))
displayHTML(ani.to_jshtml())
plt.close()

# COMMAND ----------

# DBTITLE 1,Mix Figures and Text
from base64 import b64encode
from io import BytesIO

from IPython.display import display, HTML
import matplotlib.pyplot as plt

def add_split_screen(fig, text, iwidth=None):
    figdata = BytesIO()
    fig.savefig(figdata, format='png')
    iwidth = ' width={0} '.format(iwidth) if iwidth is not None else ''
    image_data = b64encode(figdata.getvalue()).decode()
    datatable = F"""<table style="border: 1px solid black;">
                      <tr><td><img src="data:image/png;base64,{image_data}"/></td>
                          <td>{text}</td></tr></table>"""
    #display(HTML(datatable)) # Jupyter
    displayHTML(datatable) # Databricks

fig, ax = plt.subplots(1,1, figsize=(6,4))
ax.plot([1,2,4,3,5])
text = '<h4>Special Chart:</h4><BR>Description of chart will go here.'
add_split_screen(fig, text, iwidth='500px')
plt.close()

# COMMAND ----------

# MAGIC %md ## Strategy: Data Lake to Data Visualization
# MAGIC * Data Lake -> Spark Workers -> Spark Driver -> Data Visualization package -> Browser
# MAGIC   * Use Spark to access your data lake and data sources
# MAGIC   * Filter and aggregate your data, 
# MAGIC     * Reduce your data lake to a manageable number of data points for your users, your visualization and browser
# MAGIC   * Bring data in from your worker nodes to the driver with `.toPandas()`
# MAGIC   * Reminder: 
# MAGIC     * Notebook data visualization packages run on the driver node

# COMMAND ----------

# MAGIC %md #### Requires
# MAGIC `pandas==1.*, pandas_bokeh, bokeh`

# COMMAND ----------

from bokeh.embed import components, file_html
from bokeh.resources import CDN

# COMMAND ----------

# DBTITLE 0,Spark Dataframe to Data Visualization
import pandas as pd
pd.set_option('plotting.backend', 'pandas_bokeh')

# Generate a spark data set
pdf = (spark.range(1000, numPartitions=10).selectExpr("rand() as X", "rand() as Y").drop('id').toPandas())

# Databricks displayHTML, Bokeh's file_html, and CDN
displayHTML(file_html(pdf.plot_bokeh(kind="scatter", x='X', y='Y'), CDN, "Spark DF plot"))

# COMMAND ----------

# MAGIC %md ## Strategy: Install a custom hook into your graphics package

# COMMAND ----------

#from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.embed import file_html

#from bokeh.embed.notebook import notebook_content
from bokeh.io.notebook import install_notebook_hook

def databricks_show_doc(obj, state, notebook_handle):
  displayHTML(file_html(obj, CDN, "my plot1"))
  return None

def noop(a, b, c, d, **kw):
  return None

install_notebook_hook(notebook_type='databricks', load=noop, show_doc=databricks_show_doc, show_app=noop, overwrite=True)

# COMMAND ----------

# DBTITLE 1,Setup Bokeh for plotting in Databricks
from bokeh.io import output_notebook
output_notebook(notebook_type='databricks') # Set the hook

# COMMAND ----------

# DBTITLE 1,Render in Bokeh
import pandas as pd
import numpy as np

num = 95000
ts = pd.Series(np.random.randn(num), index=pd.date_range('1/1/1', periods=num)).cumsum()

## Bokeh plot setup, requires pandas > 0.25 and pandas_bokeh install
pd.set_option('plotting.backend', 'pandas_bokeh')

ts.plot()  # Now it looks like a standard pandas.plot() call

# COMMAND ----------


