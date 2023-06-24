# Databricks notebook source
# MAGIC %md ## Altair via driver-proxy
# MAGIC
# MAGIC * start a web server which enables CORS on the driver
# MAGIC * this web server can be accessed via the `/driver-proxy-api` endpoint, which allows basic auth

# COMMAND ----------

# MAGIC %pip install altair==4.1.0

# COMMAND ----------

# DBTITLE 1,Create and run simple proxy to set CORS header
# MAGIC %sh
# MAGIC set -x
# MAGIC cat > /databricks/driver/simple_cors_server.py <<EOF
# MAGIC from http.server import HTTPServer, SimpleHTTPRequestHandler, test
# MAGIC import sys
# MAGIC
# MAGIC class CORSRequestHandler (SimpleHTTPRequestHandler):
# MAGIC     def end_headers (self):
# MAGIC         self.send_header('Access-Control-Allow-Origin', '*')
# MAGIC         SimpleHTTPRequestHandler.end_headers(self)
# MAGIC
# MAGIC if __name__ == '__main__':
# MAGIC     test(CORSRequestHandler, HTTPServer, port=int(sys.argv[1]) if len(sys.argv) > 1 else 8000)
# MAGIC EOF
# MAGIC
# MAGIC # I'm using this directory as an example, but can be any tmp directory
# MAGIC cd /dbfs/FileStore/tables
# MAGIC
# MAGIC # running in backaround with `&` doesn't seem to work well, may need to cancel command, but program will be running in background
# MAGIC python /databricks/driver/simple_cors_server.py 8010 &

# COMMAND ----------

# DBTITLE 1,Verify proxy is running
# MAGIC %sh netstat -anlp |grep 8010

# COMMAND ----------

# DBTITLE 1,Create Databricks renderer for Vega Lite / altair
import urllib
def uri(data_file):
  """
    Purpose: Generate URI to access file serving service behind databricks driver proxy
    data_file: file to fetch from proxy
  """
  ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
  host    = ctx.apiUrl().get()
  cluster = ctx.clusterId().get()
  token   = ctx.apiToken().get()
  port    = 8010
  #hack the token to bypass the redaction
  return F'"{host}/driver-proxy-api/o/0/{cluster}/{port}/{data_file}?token={token[:-1]}"+"{token[-1:]}"'


import altair as alt
def databricks_renderer(spec):
  data_file = spec["data"]["url"]
  spec["data"]["url"] = "URI"
  #print(spec)
  html = alt.display.html_renderer(spec)['text/html']
  html = html.replace('"URI"',uri(data_file))
  # print( html )
  return displayHTML(html)

alt.renderers.register('databricks', databricks_renderer)
alt.renderers.enable('databricks')

# COMMAND ----------

# DBTITLE 1,standard altair rendering (should see blue scatter chart)
import altair as alt

data_file='cars.json' # previously downloaded
chart = alt.Chart(data_file) \
    .mark_point().encode(
      x='Horsepower:Q',
      y='Miles_per_Gallon:Q')

chart

# COMMAND ----------


