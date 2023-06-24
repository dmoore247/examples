# Databricks notebook source
# MAGIC %md # Visualizing Flight Paths with Python
# MAGIC
# MAGIC his is an example of plotting flight paths from the [OpenFlights dataset](http://openflights.org/data.html) with the [gcmap](https://github.com/paulgb/gcmap) Python package.
# MAGIC
# MAGIC The OpenFlights dataset contains three files, `airports.dat`, `routes.dat`, and `airlines.dat`. We are interested in the first two. We'll use `airports.dat` to get the coordinates of each airport, and routes to determine which airports are paired by a route.
# MAGIC
# MAGIC First, we use [pandas](http://pandas.pydata.org/) to load the data.
# MAGIC ## Requirements
# MAGIC * gcmap

# COMMAND ----------

# MAGIC %sh wget https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat

# COMMAND ----------

# MAGIC %sh wget https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat

# COMMAND ----------

# DBTITLE 1,Parse Airports and Route data
import pandas as pd
import numpy as np
import csv

ROUTE_COLS = ('airline_name', 'airline_id', 'source_code', 'source_id', 'dest_code', 'dest_id', 'codeshare', 'stops', 'equiptment')
AIRPORT_COLS = ('airport_id', 'airport_name', 'city', 'country', 'iata', 'icao', 'latitude', 'longitude', 'altitude', 'timezone', 'dst', 'a','b','c')

routes = pd.read_csv('routes.dat', header=None, names=ROUTE_COLS, na_values=['\\N'])
airports = pd.read_csv('airports.dat', header=None, names=AIRPORT_COLS, quoting=csv.QUOTE_ALL, sep=",")

routes.drop(['codeshare'], axis=1, inplace=True)
routes.dropna(inplace=True)
routes.source_id = routes.source_id.astype(int)
routes.dest_id = routes.dest_id.astype(int)
airports.drop({'timezone','dst','a','b','c'}, axis=1, inplace=True)
airports.airport_id = airports.airport_id.astype(int)

# COMMAND ----------

# MAGIC %md There may be multiple routes between two airports. Here we combine them into one row per pair, with a column `cnt` counting the number of times each pair appears.

# COMMAND ----------

airport_pairs = routes.groupby(('source_id', 'dest_id')).size()
airport_pairs = airport_pairs.reset_index()
airport_pairs.columns = ('source_id', 'dest_id', 'cnt')

# COMMAND ----------

# MAGIC %md Merge the `airport_pairs` table with the `airports` table to get the geocoordinates for each airport.

# COMMAND ----------

# DBTITLE 1,Join Airports and Routes
airport_pairs = airport_pairs.merge(airports, left_on='source_id', right_on='airport_id') \
                             .merge(airports, left_on='dest_id', right_on='airport_id', suffixes=('_source', '_dest'))

# COMMAND ----------

# MAGIC %md Now, bring in `GCMapper` from the `gcmap` package to render the visualization.

# COMMAND ----------

from gcmap import GCMapper
gcm = GCMapper()
gcm.set_data(
  airport_pairs.longitude_source, 
  airport_pairs.latitude_source, 
  airport_pairs.longitude_dest, 
  airport_pairs.latitude_dest, 
  airport_pairs.cnt)
gcm.width=1000
img = gcm.draw()

# COMMAND ----------

# MAGIC %md The result is a [Python Image Library](http://www.pythonware.com/products/pil/) image, which we can save as a `png` (among a handful of other formats).

# COMMAND ----------

# DBTITLE 1,Image to inline HTML
import base64
from io import BytesIO
tmpfile = BytesIO()
img.save(tmpfile, format='png')
encoded = base64.b64encode(tmpfile.getvalue()).decode("UTF-8")

html = ('<p>' + '<img src=\'data:image/png;base64,{}\'>'.format(encoded) + '</p>')

displayHTML(html)

# COMMAND ----------


