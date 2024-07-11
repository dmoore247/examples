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

# MAGIC %md #Summary of Biogen data visualization Issues and requirements
# MAGIC
# MAGIC · Erik highlighted an issue he experienced that morning regarding Databricks response to this cap and the error received 
# MAGIC
# MAGIC · Challenges: (1) Because of the way spark works, we don't get the failure message until it fails. (2) We are extending DBUs and Databricks is not giving us a valid return and we are spending more money on DBUs because Databricks is capping/locking visualizations behind an arbitrary limit.
# MAGIC
# MAGIC · Dave emphasized urgency - this has gone from a problem we might have in the future to a now blocker
# MAGIC
# MAGIC · Koushik to escalate the problem and explore potential solutions with Product Manager/Owner
# MAGIC
# MAGIC o Possible solution - bumping us to 50 MB 
# MAGIC
# MAGIC We'd like to understand in more detail your requirements, there are a few paths forward.
# MAGIC
# MAGIC 1. Max size in-line in a notebook (50MB)?
# MAGIC
# MAGIC  
# MAGIC
# MAGIC -                      Upping this cap would be immediately useful (20 to 50)
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 2. What visualization packages are you using
# MAGIC
# MAGIC  
# MAGIC
# MAGIC - Typically Plot.ly and Shiny
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 3. What type of data are you visualizing (.png, html, json, vector,...)?
# MAGIC
# MAGIC  
# MAGIC
# MAGIC -                      Currently .html, .png. Some desire to visualize DICOM data, which we can do as a .pcd file or as a heatmap in plot.ly and a need to visualize video data with annotations, which I believe compiles to .mp4 (@Angelos Karatsidis?) but which we can do a kludgy serialization/deserialization
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 4. Does this need to work in jobs and save job results? 
# MAGIC
# MAGIC  
# MAGIC
# MAGIC -                      No, this is primarily used to inspect results of ML algorithms (I.E., making sure that a image segmentation model works properly, comparing UMAP plots, etc)
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 5. Do you need to navigate and browse the data across multiple job runs?
# MAGIC
# MAGIC  
# MAGIC
# MAGIC -                      This would probably be something that we could solution towards if the visualizations worked at all
# MAGIC
# MAGIC  
# MAGIC
# MAGIC 6. Do you need access from outside Databricks, through data apps?
# MAGIC
# MAGIC  
# MAGIC
# MAGIC -                      This is a future solution (Posit Connect apps on top of Databricks)

# COMMAND ----------

# DBTITLE 1,Setup
dbutils.widgets.dropdown("data_points","5000",["100","500","5000","20000","50000", "100000", "200000", "500000", "1000000"])
points = int(dbutils.widgets.get("data_points"))
file_path = f"/Volumes/douglas_moore/demo/visuals/sine_wave_{points}.html"
points, file_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plotly redering directly to an html file saved on volumes

# COMMAND ----------

# DBTITLE 1,Plotly direct rendering in Databricks
import plotly.graph_objects as go

import numpy as np
import pandas as pd
import scipy

from scipy import signal

np.random.seed(1)

x = np.linspace(0, 10, num=points)
y = np.sin(x)
noise = 2 * np.random.random(len(x)) - 1 # uniformly distributed between -1 and 1
y_noise = y + noise

fig = go.Figure()
fig.add_trace(go.Scatter(
    x=x,
    y=y,
    mode='markers',
    marker=dict(size=2, color='black'),
    name=f'Sine ({x.shape[0]} pts)'
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

F"fig is ready to render {len(x)}"

fig.write_html(file_path)
f"Saved {file_path}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumes as durable visualization storage

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files{file_path}"
displayHTML(tmpfile)

# COMMAND ----------

# MAGIC %sh ls -lh /Volumes/douglas_moore/demo/visuals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other experiments

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_50000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_100000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_100000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_100000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_100000.html"
displayHTML(tmpfile)

# COMMAND ----------

tmpfile = f"https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals/sine_wave_100000.html"
displayHTML(tmpfile)
