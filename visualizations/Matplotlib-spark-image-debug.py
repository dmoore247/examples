# Databricks notebook source
# MAGIC %md # Data Visualization 300 with Matplotlib, Spark and Image types
# MAGIC ## Summary & Audience
# MAGIC Data Visualization strategies Solution Architects & customers need to know once they've explored Databricks display() and matplotlib in passing.
# MAGIC
# MAGIC ## Motivation
# MAGIC Analytics is complicated and requires data visualization to effectivly tell the story to a wider audience. Matplotlib is the 'defacto' standard for python visulization. Matplotlib works well in instustry standard Jupyter notebooks. Customers come to us with legacy code and need to get up and running quickly.
# MAGIC
# MAGIC ## Objective
# MAGIC Learn strategies to port not only Matplotlib visuals but also other popular Python based visualization packages to Databricks.
# MAGIC
# MAGIC ## Default Databricks rendering capabilities
# MAGIC * Spark Dataframes
# MAGIC * Matplotlib via `display()`
# MAGIC * HTML via `displayHTML()`
# MAGIC * Markdown
# MAGIC * Image data type display()
# MAGIC
# MAGIC ## Expanding visualization capabilities with 3rd party packages
# MAGIC We can adopt these strategies to extend Databricks visualization strategies both in the notebook and headless use cases such as with MLFlow. We'll start with the industry 'defacto' standard Matplotlib.
# MAGIC
# MAGIC 0. Save to PNG and embed into HTML
# MAGIC 0. Use Databricks display() and close the plot
# MAGIC 0. Render to HTML
# MAGIC 0. `%matplotlib inline`
# MAGIC 0. Log matplotlib figure to MLFlow
# MAGIC 0. Display saved figures from Markdown
# MAGIC 0. Generate figures in Spark executors

# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

# MAGIC %run "./Imaging Helper"

# COMMAND ----------

# MAGIC %md # Generate plots within Spark Executors
# MAGIC There are use cases where pre-computed plots are needed over massive data sets. Examples
# MAGIC * Generating engineering assessments of Time Series data pulled from sensor logs. The Plots are stored and served up on a real-time basis.
# MAGIC * Data visualization animations are created frame by frame

# COMMAND ----------

from PIL import Image
from pyspark.ml.image import ImageSchema

help(Image.merge)


# COMMAND ----------

# DBTITLE 1,Convert Matplotlib figure to Spark compatible Image type
from io import BytesIO
from PIL import Image
import hashlib

def figure_to_image(fig):
  tmpfile = BytesIO()
  fig.savefig(tmpfile, format='PNG') # Create a PNG byte array from Matplotlib fig

  # Convert PNG to Image data array
  img = Image.open(tmpfile).convert('RGB') # Convert to get matrix of pixel values
  r,g,b = img.split()
  imgx = Image.merge("RGB", (b, g, r)) # Flip color bands
  #print('image.size',imgx.size)
  bytesx = imgx.tobytes()
  sig = hashlib.md5(bytesx).hexdigest()
  return {
    "image": [
      F'matplotlib-{sig}.png', # origin
      img.size[1],      # height
      img.size[0],      # width
      3,                # nChannels (RGBA)
      16,               # mode
      bytesx            # must be bytearray
    ]
  }

# COMMAND ----------

# DBTITLE 1,Function to Generate Plot and convert to Image data type
import numpy as np
import matplotlib.pyplot as plt

def get_plot( a=4, b=5, points=500):
  #print(a,b,points)
  plt.close()
  print(a,b,points)
  x = np.linspace(0, 1, points)
  y = np.sin(a * np.pi * x) * np.exp(-b * x)

  fig, ax = plt.subplots()

  ax.fill(x, y, zorder=10)
  ax.grid(True, zorder=5)
  return figure_to_image(fig)

image = get_plot() # unit test
#image

# COMMAND ----------

# DBTITLE 1,Register figure generator as UDF with Image schema return type
from pyspark.sql.functions import udf
from pyspark.ml.image import ImageSchema
spark.udf.register("get_plot_udf", get_plot, ImageSchema.imageSchema)
;

# COMMAND ----------

# DBTITLE 1,Generate and display a single plot on the spark executor (not driver)
df = spark.sql("select get_plot_udf(4,5,500) as x")
display(df.select("x.image"))

# COMMAND ----------

df = spark.sql("select get_plot_udf(3,7,500) as x")
display(df.select("x.image"))

# COMMAND ----------

df = spark.sql("select get_plot_udf(0,0,500) as x")
display(df.select("x.image"))

# COMMAND ----------

# DBTITLE 1,Display plots in a dataframe
# MAGIC %sql
# MAGIC SET spark.sql.crossJoin.enabled=true;
# MAGIC WITH plots AS (
# MAGIC SELECT a.id x, b.id y, get_plot_udf(a.id, b.id, 500) as i
# MAGIC FROM 
# MAGIC   range(0,10) as a,
# MAGIC   range(0,10) as b
# MAGIC ) 
# MAGIC select x,y, i.image.origin, md5(i.image.data) checksum, i.image from plots

# COMMAND ----------

sql = """
WITH plots AS (
SELECT a.id x, b.id y, get_plot_udf(a.id, b.id, 500) as i
FROM 
  range(1,10) as a,
  range(1,10) as b
) 
select x,y, md5(i.image.data) checksum, i.image from plots
"""

# COMMAND ----------

img = spark.sql(sql).limit(2)
x = img.collect()
x

# COMMAND ----------

from PIL import Image
i = x[1]['image']
#print(i)
h = i['height']
w = i['width']
size = (w,h)
print(size)
;

data = i['data']
data = bytes(data)

img = Image.frombytes(mode='RGBA', data = data, size=size)

ImageDisplayHelper(img)

# COMMAND ----------

# MAGIC %md ## Manage plots using Delta & Spark SQL. No more small files.

# COMMAND ----------

# MAGIC %fs rm -r /tmp/matplotlib_figures

# COMMAND ----------

spark.sql(sql).write.format('delta').mode('overwrite').save('/tmp/matplotlib_figures')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/tmp/matplotlib_figures` where x between 1 and 6 and y = 7
# MAGIC order by x, y

# COMMAND ----------

# MAGIC %sql select * from delta.`/tmp/matplotlib_figures` where x = 1 and y = 7

# COMMAND ----------

# MAGIC %sql select * from delta.`/tmp/matplotlib_figures` where x = 6 and y = 7

# COMMAND ----------

# MAGIC %sql OPTIMIZE delta.`/tmp/matplotlib_figures`

# COMMAND ----------

# MAGIC %sql select c.image from delta.`/tmp/matplotlib_figures` where id between 50 and 55

# COMMAND ----------

# MAGIC %sql select c.image from delta.`/tmp/matplotlib_figures`

# COMMAND ----------


