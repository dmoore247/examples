# Databricks notebook source
# MAGIC %md
# MAGIC # Altair display demo
# MAGIC ## Summary
# MAGIC ![](https://avatars2.githubusercontent.com/u/22396732?s=200&v=4) Altair is a charting package popular with statistical analysts using python.</br>
# MAGIC https://altair-viz.github.io/#altair-declarative-visualization-in-python
# MAGIC
# MAGIC
# MAGIC ## Requirements
# MAGIC * Requires altair library installed on ML runtime cluster
# MAGIC * Requires vega_datasets installed on ML runtime cluster
# MAGIC * Tested on 5.5 LTS ML runtime with `altair`, `vega_datasets` python package installed
# MAGIC
# MAGIC ### Note:
# MAGIC * In-line data or data hosted on Github works
# MAGIC * Data files hosted on dbfs:/FileStore/ does not work due to CORS security restrictions

# COMMAND ----------

# MAGIC %pip install altair altair_saver vega_datasets selenium webdriver-manager

# COMMAND ----------

# DBTITLE 1,Imports
import altair as alt
from vega_datasets import data

# COMMAND ----------

# DBTITLE 1,Altair Interactive Chart with Cars data set
import altair as alt
from vega_datasets import data

alt.renderers.enable('html')
alt.data_transformers.enable(max_rows=5000) # Default max rows

source = data.cars() # pandas DF

brush = alt.selection(type='interval')
points = alt.Chart(source).mark_point().encode(
    x='Horsepower:Q',
    y='Miles_per_Gallon:Q',
    color=alt.condition(brush, 'Origin:N', alt.value('lightgray'))
).add_selection(
    brush
)

bars = alt.Chart(source).mark_bar().encode(
    y='Origin:N',
    color='Origin:N',
    x='count(Origin):Q'
).transform_filter(
    brush
)

points & bars # Jupyter
#displayHTML((points & bars).to_html())

# COMMAND ----------

# DBTITLE 1,Altair with Vega Datasets Github hosted zipcodes.csv
import altair as alt
from vega_datasets import data

# Since the data is more than 5,000 rows we'll import it from a URL
source = data.zipcodes.url
print(source)

chart = alt.Chart(source).transform_calculate(
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
displayHTML(chart.to_html())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Host large data sets and serve them up via FileStore for Altair
# MAGIC https://docs.databricks.com/data/filestore.html
# MAGIC
# MAGIC DBFS has a `dbfs:/FileStore` path which is accessible to the browser of the logged in user of Databricks. This is a place to store images, html, generated files for download.
# MAGIC This next section will download a data file and display the locally hosted file via Altair
# MAGIC

# COMMAND ----------

# DBTITLE 1,Download zipcodes.csv
# MAGIC %sh 
# MAGIC wget https://vega.github.io/vega-datasets/data/zipcodes.csv
# MAGIC mkdir -p /dbfs/FileStore/dmoore/
# MAGIC cp zipcodes.csv /dbfs/FileStore/dmoore/zipcodes.csv
# MAGIC rm zipcodes.csv

# COMMAND ----------

displayHTML("""<a href="files/dmoore/zipcodes.csv">Verify Zipcodes.csv download</a>""")

# COMMAND ----------

# DBTITLE 1,By Default, Databricks blocks Altair JavaScript from pulling zipcodes.csv from the FileStore
import altair as alt

# Since the data is more than 5,000 rows we'll import it from a URL
source = 'files/dmoore/zipcodes.csv'

chart = alt.Chart(source).transform_calculate(
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
print("inspect source, look at console to view CORS related errors")
displayHTML(chart.to_html())

# COMMAND ----------

# MAGIC %md ## Rendering of large datasets doesn't work by default
# MAGIC * Altair will pull files from the webserver using thirdparty JavaScript
# MAGIC * Databricks implements CORS protections by default
# MAGIC * Result is the plots are blocked
# MAGIC * Use Chrome -> Right Mouse -> inspect -> view console to see the CORS error messages

# COMMAND ----------

# DBTITLE 1,Doesn't work either by saving to chart.html
target = "/dbfs/FileStore/dmoore/chart.html"
chart.save(target)
displayHTML('<a href="https://demo.cloud.databricks.com/files/dmoore/chart.html">chart</a>')

# COMMAND ----------

# MAGIC %md ## Fixing the Altair large data set rendering problem
# MAGIC 1. Bumping up record limits
# MAGIC   * Reduce data frame size
# MAGIC 0. Save as HTML
# MAGIC 0. Save as PNG
# MAGIC 0. Save as SVG
# MAGIC 0. Rendering using a proxy
# MAGIC   
# MAGIC This reference is helpful https://altair-viz.github.io/user_guide/faq.html

# COMMAND ----------

# MAGIC %md ### Bumping up record limits

# COMMAND ----------

# DBTITLE 1,Set max_rows=50,000 and selected only zipcode, lat, long
import altair as alt
from vega_datasets import data

# Since the data is more than 5,000 rows, we'll bump up the limit, up to max 20MB
alt.data_transformers.enable(max_rows=50000) # Default max rows
source = data.zipcodes([['zipcode','latitude','longitude']])

chart = alt.Chart(source).transform_calculate(
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
displayHTML(chart.to_html())

# COMMAND ----------

# DBTITLE 1,Save as HTML then display
chart.save('/dbfs/FileStore/plots/chart.html')
import os
with open('/dbfs/FileStore/plots/chart.html','r') as f:
  displayHTML(f.read())

# COMMAND ----------

# MAGIC %md ### Render Altair chart as PNG image or SVG
# MAGIC Rendering as PNG or SVG requires a 'headless' webrowser (chrome) and selenium to drive it

# COMMAND ----------

# DBTITLE 1,Install Chrome & Chromedriver
# MAGIC %sh
# MAGIC
# MAGIC wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add - 
# MAGIC sudo sh -c 'echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
# MAGIC sudo apt-get update 
# MAGIC sudo apt-get -yqq install google-chrome-stable
# MAGIC
# MAGIC sudo apt-get -yqq install chromium-chromedriver
# MAGIC conda install -c conda-forge altair_saver

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import altair as alt
import pandas as pd
from altair_saver import save

alt.data_transformers.enable(max_rows=50000) # Default max rows
altair_saver.available_formats()

# COMMAND ----------

# DBTITLE 1,Chart, Save as PNG, Display
source = pd.read_csv("/dbfs/FileStore/dmoore/zipcodes.csv")[['zip_code','latitude','longitude']]

chart = (alt.Chart(source)
           .transform_calculate("leading digit",alt.expr.substring(alt.datum.zip_code, 0, 1))
           .mark_circle(size=3)
           .encode(longitude='longitude:Q',latitude='latitude:Q',color='leading digit:N',tooltip='zip_code:N')
           .project(type='albersUsa')
           .properties(width=650,height=400)
        )

save(chart, '/dbfs/FileStore/plots/douglas.moore/chart.png')
displayHTML('<img src="files/plots/douglas.moore/chart.png">')

# COMMAND ----------

# DBTITLE 1,Save as SVG
# WARNING: This could explode the size of the notebook

save(chart, '/dbfs/FileStore/plots/douglas.moore/chart.svg')
import os
with open('/dbfs/FileStore/plots/douglas.moore/chart.svg','r') as f:
  displayHTML(f.read())

# COMMAND ----------

# DBTITLE 1,Save as PDF
target = "/dbfs/FileStore/plots/douglas.moore/chart.pdf"
url = "https://demo.cloud.databricks.com/files/plots/douglas.moore/chart.pdf"
save(chart, target)

displayHTML(F"""<a type="application/pdf" download="chart.pdf" target="_self" href="{url}">Hold down command key to download chart</a>""")

# COMMAND ----------

# MAGIC %md ## Rendering Altair via Driver Proxy
# MAGIC To use the driver proxy take a look at this example: <p><a href="#./altair-altair_data_server-databricks-X">altair-altair_data_server-databricks-X</a>

# COMMAND ----------


