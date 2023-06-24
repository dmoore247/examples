# Databricks notebook source
# MAGIC %md # Altair rendering of large datasets via driver-proxy
# MAGIC * Altair is a popular declarative JavaScript based charting package. 
# MAGIC * Altair is used to render high quality interactive charts.
# MAGIC * For charts of small data sets, less than `max_rows` (default 5000), Altair will render the chart with the JSON data structure inline to the HTML.
# MAGIC * For charts exeeding `max_rows`, Altair will write the data to a separate .json file and render the HTML & JavaScript with a reference to the separate json file. 
# MAGIC   * The Altair JavaScript libraries will attempt to fetch the data and render the chart in the browser.  
# MAGIC   * This works well for a local Jupyter notebook. 
# MAGIC   * This doesn't work well with Databricks WebApp. 
# MAGIC   * The Databricks WebApp blocks the CORS request, also the request is coming from databricksusercontent.com because that's where `displayHTML()` content is rendered from.
# MAGIC
# MAGIC ### Use a proxy
# MAGIC * We will use `altair_data_server` to run an HTTP compatible data service on the driver node
# MAGIC * We will access the data service via the Databricks `/driver-proxy-api` endpoint, which allows basic auth
# MAGIC * The `altair_data_server` will generate the headers necessary to avoid the CORS errors and serve the json/csv data files
# MAGIC * We provide a 'databricks' renderer type to generate the HTML and fix the proxy URL for the databricks proxy
# MAGIC
# MAGIC ## Requires
# MAGIC * `altair==4.0.0`
# MAGIC * `altair_data_server`
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC * May not work on Azure Databricks
# MAGIC * If no charts are displaying, it's possible that the token has expired or has been revoked
# MAGIC
# MAGIC ## Warning
# MAGIC * This is a prototype only, the token handling needs to be make more robust and secure

# COMMAND ----------

# MAGIC %md ## Setup configuration
# MAGIC Install Altair libraries
# MAGIC
# MAGIC Grab workspace host and cluster from scala and pass to Python via Spark Context

# COMMAND ----------

# DBTITLE 1,Import Altair libraries
#dbutils.library.installPyPI('altair', '4.0.0')  # Data Visualization library that uses XDR/JSON to fetch out of band data
#dbutils.library.installPyPI('altair_data_server')  # data service for systems requiring a proxy

# COMMAND ----------

# DBTITLE 1,Inject needed host, cluster, token properties into spark context
# MAGIC %scala
# MAGIC spark.conf.set("ws_host", dbutils.notebook.getContext().apiUrl.get)
# MAGIC spark.conf.set("ws_cluster", dbutils.notebook.getContext().clusterId.get)
# MAGIC //spark.conf.set("ws_token", dbutils.secrets.get("content-token","dmoore"))
# MAGIC spark.conf.set("ws_token", "dapie55d9d70385e9bcb74b5d2cf0b215904") // TODO: Put your own token here, this one has been revoked

# COMMAND ----------

# DBTITLE 1,Define HTML template
 html_template = """<!DOCTYPE html>
<html>
<head>
  <style>
    .error {
        color: red;
    }
  </style>
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm//vega@5"></script>
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm//vega-lite@4.0.0"></script>
  <script type="text/javascript" src="https://cdn.jsdelivr.net/npm//vega-embed@6"></script>
</head>
<body>
  <div id="vis"></div>
  <script>
    (function(vegaEmbed) {
      var spec = {spec};
      var embedOpt = {"mode": "vega-lite"};

      function showError(el, error){
          el.innerHTML = ('<div class="error" style="color:red;">'
                          + '<p>JavaScript Error: ' + error.message + '</p>'
                          + "<p>This usually means there's a typo in your chart specification. "
                          + "See the javascript console for the full traceback.</p>"
                          + '</div>');
          throw error;
      }
      const el = document.getElementById('vis');
      vegaEmbed("#vis", spec, embedOpt)
        .catch(error => showError(el, error));
    })(vegaEmbed);

  </script>
</body>
</html>"""

# COMMAND ----------

# DBTITLE 1,Databricks Altair proxy utilities
import altair_data_server
import altair as alt
import json
def generate_html(spec): # inject altair spec into html template
  return html_template.replace('{spec}',json.dumps(spec))

def proxy_uri(file_path, proxy_port): # format proxy URI
  return '{workspace_host}/driver-proxy-api/o/0/{workspace_clusterId}/{proxy_port}/{file_path}?token={token}' \
  .format(workspace_host=spark.conf.get("ws_host"),  
          workspace_clusterId=spark.conf.get("ws_cluster"), 
          proxy_port=proxy_port, 
          file_path=file_path, 
          token=spark.conf.get("ws_token"))

# Extend Altair to render in Databricks using displayHTML
def databricks_renderer(spec):
  altair_proxy_list = spec['data']['url'].split('/') # Pos 2 is port, pos 3 is filename
  spec['data']['url'] = proxy_uri(altair_proxy_list[3], altair_proxy_list[2])
  displayHTML(generate_html(spec))
  return {}

alt.renderers.register('databricks', databricks_renderer)

# COMMAND ----------

# MAGIC %md ## Plot large Pandas dataset

# COMMAND ----------

# DBTITLE 1,Plot 'big' dataset
import altair as alt
alt.renderers.enable('databricks')
alt.data_transformers.enable('data_server_proxied')

# 'Big' dataset
n = 50000
import pandas as pd, numpy as np
df = pd.DataFrame({ 'x': np.arange(n), 'y': np.random.randn(n),})

#Altair chart definition
chart = alt.Chart(df).mark_point().encode(
  x='x:Q',
  y='y:Q',
  tooltip=['x', 'y']
).properties(
  description='This is a simple large chart with {} points'.format(n),
  width=800,
  height=600
)

chart  # render it like you would in Jupyter (notice no .to_html(), no .display(), no displayHTML())

# COMMAND ----------

# MAGIC %md ## Render large CSV based dataset

# COMMAND ----------

# DBTITLE 1,Altair render dataset from zipcodes.csv
import altair as alt
import pandas as pd

# setup rendering on Databricks
alt.renderers.enable('databricks')
alt.data_transformers.enable('data_server_proxied')

source = pd.read_csv("/dbfs/FileStore/dmoore/zipcodes.csv")[['zip_code','latitude','longitude']]

zipcodes = alt.Chart(source).transform_calculate(
    "leading digit", alt.expr.substring(alt.datum.zip_code, 0, 1)
).mark_circle(size=3).encode(
    longitude='longitude:Q',
    latitude='latitude:Q',
    color='leading digit:N',
    tooltip='zip_code:N'
).project(
    type='albersUsa'
).properties(
    width=650,
    height=400
)
zipcodes

# COMMAND ----------

# DBTITLE 1,Altair Interactive visualization of large dataset
import altair as alt
alt.renderers.enable('databricks')
alt.data_transformers.enable('data_server_proxied')

#alt.renderers.enable('default')
#alt.data_transformers.enable('default')


# 'Big' dataset
n = 50000
import pandas as pd, numpy as np
df = pd.DataFrame({ 'x': np.arange(n), 'y': np.random.randn(n),})

#Altair chart definition
brush = alt.selection(type='interval', encodings=['x'])
chart = alt.Chart(df).mark_point().encode(
  x='x:Q',
  y='y:Q',
  tooltip=['x', 'y'],
  color=alt.condition(brush, 'Origin:N', alt.value('lightgray'))
).properties(
  description='This is a simple large chart with {} points'.format(n),
  width=800,
  height=600
).add_selection(
    brush
)

bars = alt.Chart(df).mark_bar().encode(
    y='Origin:N',
    color='Origin:N',
    x='count(Origin):Q'
).transform_filter(
    brush
)

chart & bars
#chart  # draw it

# COMMAND ----------


