# Databricks notebook source
# MAGIC %md # Pandas.plot()
# MAGIC [PyViz.org](https://pyviz.org/index.html) is about unifying plotting packages and plotting backends within Python.
# MAGIC This notebook will demonstrate a few of the backends:
# MAGIC * Matplotlib
# MAGIC * Plotly (since 0.25)
# MAGIC * Hvplot
# MAGIC * Bokeh

# COMMAND ----------

# DBTITLE 1,Setup latest Pandas version and required data visualization packages
# MAGIC %conda install bokeh==1.4.0 hvplot pandas==1.* holoviews hvplot

# COMMAND ----------

# MAGIC %conda install -c plotly plotly_express==0.4.0 plotly==4.8.1

# COMMAND ----------

import matplotlib.pyplot as plt
plt.close('all')

# COMMAND ----------

import pandas
pandas.__version__

# COMMAND ----------

# MAGIC %md ## Pandas - Matplotlib

# COMMAND ----------

import pandas as pd
import numpy as np
pd.options.plotting.backend = "matplotlib"

ts = pd.Series(np.random.randn(1000),
                index=pd.date_range('1/1/2000', periods=1000))
ts = ts.cumsum()

ts.plot()

# COMMAND ----------

pd.options.plotting.backend

# COMMAND ----------

# MAGIC %md ## Pandas - Plotly

# COMMAND ----------

import pandas as pd
import plotly
pd.__version__,plotly.__version__

# COMMAND ----------

import pandas as pd
import numpy as np
import plotly

##  plot setup, requires pandas >= 0.25, plotly >= 4.8.1
pd.set_option('plotting.backend', 'plotly')

num = 1000
ts = pd.Series(np.random.randn(num), index=pd.date_range('1/1/2000', periods=num)).cumsum()

ts.plot()

# COMMAND ----------

# MAGIC %md ## Pandas DataFrame.plot() with Holoviews-Bokeh backend
# MAGIC * Create HTML helper function for Databricks to display using HTML memory buffer
# MAGIC * Run plot using helper function

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

import math
import numpy as np
import pandas as pd
import holoviews as hv
from bokeh.embed import components, file_html
from bokeh.resources import CDN
import hvplot

# COMMAND ----------

hv.extension('bokeh')
renderer = hv.renderer('bokeh').instance(fig='html', holomap='auto')
pd.options.plotting.backend = "hvplot"

# see https://github.com/ioam/holoviews/issues/1819
def displayHoloviews(hv_plot, html_name="plot.html", width=1000, height=600, renderer=renderer):
  plot = renderer.get_plot(hv_plot).state
  setattr(plot, 'plot_width', width)
  setattr(plot, 'plot_height', height)
  html = file_html(plot, CDN, html_name)
  pos = html.find("__ndarray__")
  print(len(html), html[pos:pos+100])
  displayHTML(file_html(plot, CDN, html_name))

import hvplot.pandas  # noqa
num = 95000
ts = pd.Series(np.random.randn(num), index=pd.date_range('1/1/1', periods=num))
ts = ts.cumsum()

displayHoloviews(ts.plot())

# COMMAND ----------

# MAGIC %md ## Pandas Bokeh plots
# MAGIC Reference: [Pandas Bokeh](https://github.com/PatrikHlobil/Pandas-Bokeh#geoplots)
# MAGIC
# MAGIC Python: [pandas-bokeh](https://pypi.org/project/pandas-bokeh/)

# COMMAND ----------

import pandas as pd
import numpy as np

ts = pd.Series(np.random.randn(1000),
                index=pd.date_range('1/1/2000', periods=1000))
ts = ts.cumsum()


## Bokeh plot setup
pd.set_option('plotting.backend', 'pandas_bokeh')

# Needed to render HTML
from bokeh.embed import components, file_html
from bokeh.resources import CDN

# Databricks displayHTML, Bokeh's file_html, and CDN
displayHTML(file_html(ts.plot_bokeh(), CDN, "my plot1"))

# COMMAND ----------

# MAGIC %md ## pandas-bokeh on a Spark DataFrame

# COMMAND ----------

from pyspark.sql.functions import expr
df = spark.range(10).withColumn('val',expr("2*id"))

# Needed to render HTML
from bokeh.embed import components, file_html
from bokeh.resources import CDN

# Databricks displayHTML, Bokeh's file_html, and CDN
displayHTML(
  file_html(
    df.plot_bokeh(kind="scatter", x='id', y='val')
    , CDN, "Spark DF plot"
  )
)

# COMMAND ----------

# MAGIC %md ## How to get a Pandas DataFrame from a Spark DataFrame

# COMMAND ----------

assert 'true' == spark.conf.get("spark.sql.execution.arrow.enabled")

# COMMAND ----------

pdf = (
  spark.range(1000)
    .selectExpr("rand() as X", "rand() as Y")
    .drop('id')
    .toPandas()
)

# Needed to render HTML
from bokeh.embed import components, file_html
from bokeh.resources import CDN

# Databricks displayHTML, Bokeh's file_html, and CDN
displayHTML(
  file_html(
    pdf.plot_bokeh(kind="scatter", x='X', y='Y')
    , CDN, "Spark DF plot"
  )
)

# COMMAND ----------

# MAGIC %md ## Setup Vega backend with pgvega

# COMMAND ----------

# %conda install pandas==0.25.* pdvega vega_datasets

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas
import pdvega
pandas.__version__

# COMMAND ----------

# DBTITLE 1,Define displayVega()
import json
import pdvega
def displayVega(p: pdvega._axes.Axes)-> None:
  """
    Display Vega spec from Vega Axes Class (<class 'pdvega._axes.Axes'>)
  """
  displayHTML(
  """
<html>
<head>
  <title>Embedding Vega-Lite</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vega/3.0.0/vega.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vega-lite/2.0.0-beta.14/vega-lite.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vega-embed/3.0.0-beta.20/vega-embed.js"></script>
</head>
<body>

  <div id="vis"></div>

  <script type="text/javascript">
    var yourVlSpec = {spec}
    vega.embed("#vis", yourVlSpec);
  </script>
</body>
</html>
""".format(spec=p.spec)
)

# COMMAND ----------

# MAGIC %matplotlib inline

# COMMAND ----------

import matplotlib.pyplot as plt

from vega_datasets import data
iris = data.iris()

p = iris.vgplot(kind='scatter', x='sepalLength', y='petalLength', c='species')
displayVega(p)

# COMMAND ----------

# DBTITLE 1,Pandas with Vega plotting backend
import numpy as np
import pandas as pd
import pdvega  # import adds vgplot attribute to pandas

pdf = pd.DataFrame({'x': np.random.randn(100), 'y': np.random.randn(100)})

displayVega(pdf.vgplot(kind='scatter', x='x', y='y'))

# COMMAND ----------


