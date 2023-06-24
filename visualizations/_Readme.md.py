# Databricks notebook source
# MAGIC %md # Popular Third Party Python Visualization Packages on Databricks
# MAGIC
# MAGIC Analytics is about telling a powerful story. Visuals, charts, graphs are a key aspect of telling that story to a wide audience. There are a plethora of Python packages that help data scientists, data engineers and business analysist turn data into information and understanding.
# MAGIC This series will explore strategies for building portable Python based visuals compatible with the wide range of popular Python data visualization packages.
# MAGIC
# MAGIC There are a zoo of powerful Python visualization packages and it's not obvious how to port those packages to Databricks notebooks or how to run them at scale in a headless manner.
# MAGIC ![Python Visualization Landscape](https://www.anaconda.com/wp-content/uploads/2019/01/PythonVisLandscape.jpg)
# MAGIC
# MAGIC *Reference: https://www.anaconda.com/python-data-visualization-2018-why-so-many-libraries/*
# MAGIC
# MAGIC This series of Databricks notebooks demonstrate how to run popular 3rd party charting packages on Databricks using display() and displayHTML() capabilities. There are several tricks employed to port IPython based visuals to Databricks, including the use of in-memory image generation, exploiting lesser known HTML and SVG renderers.
# MAGIC
# MAGIC These notebooks run on Databricks DBR 6.x ML and python 3
# MAGIC
# MAGIC <p><a href="#notebook/4806770/command/4813946">Altair</a> is a charting package popular with statistical analysts using python.</p>
# MAGIC <p><a href="#notebook/2739690/command/2818916">Bokeh</a> is an interactive visualization library for modern web browsers. It provides elegant, concise construction of versatile graphics, and affords high-performance interactivity over large or streaming datasets. Bokeh can help anyone who would like to quickly and easily make interactive plots, dashboards, and data applications.</p>
# MAGIC   
# MAGIC <p><a href="#notebook/5098925/command/5098926">D3.js</a> is a JavaScript library for manipulating documents based on data. D3 helps you bring data to life using HTML, SVG, and CSS. D3’s emphasis on web standards gives you the full capabilities of modern browsers without tying yourself to a proprietary framework, combining powerful visualization components and a data-driven approach to DOM manipulation.</p>
# MAGIC   
# MAGIC <p><a href="#notebook/4956638/command/4956720"> Datashader </a> is a graphics pipeline system for creating meaningful representations of large datasets quickly and flexibly.</p>
# MAGIC <p><a href="#notebook/5098981/command/5099048">Folium</a> is a popular package for display geographical map based visuals</p>
# MAGIC <p><a href="#notebook/5099055/command/5099069">Gcmap</a> for great circle map projections. The demo displays airport routes overlayed on black global map.</p>
# MAGIC <p><a href="#notebook/4956953">Geoviews</a> for displaying high density geographical information</p>
# MAGIC <p><a href="#notebook/4951843">Graphviz</a> generalized network charting package supporting DOT expression</p>
# MAGIC <p><a href="#notebook/4956383">Holoviews + Matplotlib</a> backend and <a href="#notebook/4956125"> example 2</a></p>
# MAGIC <p><a href="#notebook/5098968">Holoviews + Bokeh + hvplot + Pandas</a> example putting together very powerful python packages
# MAGIC <p><a href="#notebook/5098942">Holoviews + Bokeh + NetworkX</a> network visualization example
# MAGIC <p><a href="#notebook/4951919">Matplotlib</a> simple examples, one embedding into HTML and the other using the Matplotlib backend and Databricks display()</p>
# MAGIC <p><a href="#notebook/5175708">Matplotlib+Spark</a> Parallel rendering of Matplotlib plots inside Spark UDFs
# MAGIC <p><a href="#notebook/5098896">Pandas + Matplotlib | Bokeh</a> Exploring two backends to Pandas based charting</p>
# MAGIC <p><a href="#notebook/5100714">Plotly</a> Community edition of Plot.ly rendered native to Databricks, to HTML and to an image.
# MAGIC <p><a href="#notebook/2739695">Seaborn</a> for building sophisticated and attractive charts</p>
# MAGIC <p><a href="#notebook/5099282">Vega</a> is a declarative format for creating, saving, and sharing visualization designs. With Vega, visualizations are described in JSON, and generate interactive views using either HTML5 Canvas or SVG.
# MAGIC <p><a href="#notebook/4951895">Pyvis / Visjs</a> for drawing network and network based charts. This noteboook has a demo of a data lineage network. </p>

# COMMAND ----------

# MAGIC %sh python --version

# COMMAND ----------

# MAGIC %sh pip freeze

# COMMAND ----------


