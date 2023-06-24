# Databricks notebook source
# MAGIC %md ## Altair via driver-proxy
# MAGIC
# MAGIC * start a web server which enables CORS on the driver
# MAGIC * this web server can be accessed via the `/driver-proxy-api` endpoint, which allows basic auth

# COMMAND ----------

# MAGIC %pip install altair==4.1.0 vega_datasets

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

# MAGIC %sh kill 42894

# COMMAND ----------

import pandas as pd
import numpy as np
import altair as alt
#alt.renderers.enable('html')
n = 50000 # works
#n = 500000 # fails, result too large
alt.data_transformers.enable(max_rows=n)
df = pd.DataFrame({
    'x': np.arange(n),
    'y': np.random.randn(n),
})

big_chart = alt.Chart(df).mark_line().encode(
    x='x',
    y='y'
)

big_chart

# COMMAND ----------

import altair as alt
from vega_datasets import data

# Since these data are each more than 5,000 rows we'll import from the URLs
airports = data.airports.url
flights_airport = data.flights_airport.url

states = alt.topo_feature(data.us_10m.url, feature="states")

# Create mouseover selection
select_city = alt.selection_single(
    on="mouseover", nearest=True, fields=["origin"], empty="none"
)

# Define which attributes to lookup from airports.csv
lookup_data = alt.LookupData(
    airports, key="iata", fields=["state", "latitude", "longitude"]
)

background = alt.Chart(states).mark_geoshape(
    fill="lightgray",
    stroke="white"
).properties(
    width=750,
    height=500
).project("albersUsa")

connections = alt.Chart(flights_airport).mark_rule(opacity=0.35).encode(
    latitude="latitude:Q",
    longitude="longitude:Q",
    latitude2="lat2:Q",
    longitude2="lon2:Q"
).transform_lookup(
    lookup="origin",
    from_=lookup_data
).transform_lookup(
    lookup="destination",
    from_=lookup_data,
    as_=["state", "lat2", "lon2"]
).transform_filter(
    select_city
)

points = alt.Chart(flights_airport).mark_circle().encode(
    latitude="latitude:Q",
    longitude="longitude:Q",
    size=alt.Size("routes:Q", scale=alt.Scale(range=[0, 1000]), legend=None),
    order=alt.Order("routes:Q", sort="descending"),
    tooltip=["origin:N", "routes:Q"]
).transform_aggregate(
    routes="count()",
    groupby=["origin"]
).transform_lookup(
    lookup="origin",
    from_=lookup_data
).transform_filter(
    (alt.datum.state != "PR") & (alt.datum.state != "VI")
).add_selection(
    select_city
)

(background + connections + points).configure_view(stroke=None)

# COMMAND ----------

import altair as alt

# COMMAND ----------


from vega_datasets import data
import requests

# Since these data are each more than 5,000 rows we'll download from the URLs

r = requests.get(data.airports.url, allow_redirects=True)
open('/dbfs/FileStore/tables/airports.csv', 'wb').write(r.content)


r = requests.get(data.us_10m.url, allow_redirects=True)
open('/dbfs/FileStore/tables/us-10m.json', 'wb').write(r.content)

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
  # return F'"{host}/driver-proxy-api/o/0/{cluster}/{port}/{data_file}?token={token[:-1]}"+"{token[-1:]}"'
  return F'{host}/driver-proxy-api/o/0/{cluster}/{port}/{data_file}?token=__TOKEN__'


import altair as alt

def databricks_renderer(spec):
  #print(spec)
  ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
  token   = ctx.apiToken().get()
  html = alt.display.html_renderer(spec)['text/html']
  html = html.replace('__TOKEN__',F'{token[:-1]}"+"{token[-1:]}')
  return displayHTML(html)

alt.renderers.register('databricks', databricks_renderer)
alt.renderers.enable('databricks')

# COMMAND ----------

data.flights_airport.url, data.airports.url

# COMMAND ----------

airports = data.airports.url
#airports = uri('airports.csv')

flights_airport = data.flights_airport.url

states = alt.topo_feature(data.us_10m.url, feature="states")

# Create mouseover selection
select_city = alt.selection_single(
    on="mouseover", nearest=True, fields=["origin"], empty="none"
)

# Define which attributes to lookup from airports.csv
lookup_data = alt.LookupData(
    airports, key="iata", fields=["state", "latitude", "longitude"]
)

background = alt.Chart(states).mark_geoshape(
    fill="lightgray",
    stroke="white"
).properties(
    width=750,
    height=500
).project("albersUsa")

connections = alt.Chart(flights_airport).mark_rule(opacity=0.35).encode(
    latitude="latitude:Q",
    longitude="longitude:Q",
    latitude2="lat2:Q",
    longitude2="lon2:Q"
).transform_lookup(
    lookup="origin",
    from_=lookup_data
).transform_lookup(
    lookup="destination",
    from_=lookup_data,
    as_=["state", "lat2", "lon2"]
).transform_filter(
    select_city
)

points = alt.Chart(flights_airport).mark_circle().encode(
    latitude="latitude:Q",
    longitude="longitude:Q",
    size=alt.Size("routes:Q", scale=alt.Scale(range=[0, 1000]), legend=None),
    order=alt.Order("routes:Q", sort="descending"),
    tooltip=["origin:N", "routes:Q"]
).transform_aggregate(
    routes="count()",
    groupby=["origin"]
).transform_lookup(
    lookup="origin",
    from_=lookup_data
).transform_filter(
    (alt.datum.state != "PR") & (alt.datum.state != "VI")
).add_selection(
    select_city
)

(background + connections + points).configure_view(stroke=None)

# COMMAND ----------


