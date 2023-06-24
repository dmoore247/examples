# Databricks notebook source
# MAGIC %md # Plotly
# MAGIC ![Plotly](https://raw.githubusercontent.com/cldougl/plot_images/add_r_img/plotly_2017.png)
# MAGIC
# MAGIC JavaScript based data visualization package with a commercial company for support and enterprise interactive 'Dash' upgrades available
# MAGIC Recent addition to Databricks Runtime now supports Plotly
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 0. Direct rendering in Databricks
# MAGIC 0. Headless rendering using HTML
# MAGIC 0. Headless renderinging generating an inline PNG image using an X Windows base backend rendering engine

# COMMAND ----------

# DBTITLE 1,Setup
dbutils.widgets.dropdown("points","5000",["100","500","5000","20000","50000"])
points = int(dbutils.widgets.get("points"))

# COMMAND ----------

# DBTITLE 1,Plotly direct rendering in Databricks
import plotly.graph_objects as go

import numpy as np
import pandas as pd
import scipy

from scipy import signal

np.random.seed(1)

x = np.linspace(0, 10, 100)
y = np.sin(x)
noise = 2 * np.random.random(len(x)) - 1 # uniformly distributed between -1 and 1
y_noise = y + noise

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=x,
    y=y,
    mode='markers',
    marker=dict(size=2, color='black'),
    name='Sine'
))

fig.add_trace(go.Scatter(
    x=x,
    y=y_noise,
    mode='markers',
    marker=dict(
        size=6,
        color='green',
        symbol='circle-open'
    ),
    name='Noisy Sine'
))

fig.add_trace(go.Scatter(
    x=x,
    y=signal.savgol_filter(y,
                           53, # window size used for filtering
                           3), # order of fitted polynomial
    mode='markers',
    marker=dict(
        size=6,
        color='mediumpurple',
        symbol='triangle-up'
    ),
    name='Savitzky-Golay'
))


fig.show() # Now works in DBR

# COMMAND ----------

# DBTITLE 1,Interactive display with headless rendering using .write_html()
import base64
from io import StringIO
tmpfile = StringIO()

fig.write_html(tmpfile)
displayHTML(tmpfile.getvalue())

# COMMAND ----------

# DBTITLE 1,Install Image generation dependencies
# MAGIC %sh 
# MAGIC apt-get install xvfb -y  # install X Windows frame buffer required by orca
# MAGIC /databricks/conda/bin/conda install -c plotly plotly-orca psutil # install ocra backend required to generate images

# COMMAND ----------

# DBTITLE 1,Headless to Image (requires orca package)
import plotly.io as pio
pio.orca.config.use_xvfb = True

img = fig.to_image(format='png')

# Render image in-line
encoded = base64.b64encode(img).decode("UTF-8")
html = ('<p>' + '<img src=\'data:image/png;base64,{}\'>'.format(encoded) + '</p>')

displayHTML(html)

# COMMAND ----------

# MAGIC %sh find /databricks -name conda

# COMMAND ----------

print(points)

# COMMAND ----------

# DBTITLE 1,Plot.ly with 50,000 values
import plotly.graph_objects as go

import numpy as np
import pandas as pd
import scipy

from scipy import signal

np.random.seed(1)

x = np.linspace(0, 10, points)
y = np.sin(x)
noise = 2 * np.random.random(len(x)) - 1 # uniformly distributed between -1 and 1
y_noise = y + noise

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=x,
    y=y,
    mode='markers',
    marker=dict(size=2, color='black'),
    name='Sine'
))

fig.add_trace(go.Scatter(
    x=x,
    y=y_noise,
    mode='markers',
    marker=dict(
        size=6,
        color='red',
        symbol='circle-open'
    ),
    name='Noisy Sine'
))

fig.add_trace(go.Scatter(
    x=x,
    y=signal.savgol_filter(y,
                           53, # window size used for filtering
                           3), # order of fitted polynomial
    mode='markers',
    marker=dict(
        size=6,
        color='mediumpurple',
        symbol='triangle-up'
    ),
    name='Savitzky-Golay'
))


fig.show() # Now works in DBR

# COMMAND ----------


