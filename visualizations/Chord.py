# Databricks notebook source
# MAGIC %md <div class="contentcontainer med left" style="margin-left: -50px;">
# MAGIC <dl class="dl-horizontal">
# MAGIC   <dt>Title</dt> <dd> Graph Element</dd>
# MAGIC   <dt>Dependencies</dt> <dd>Matplotlib</dd>
# MAGIC   <dt>Backends</dt> 
# MAGIC     <dd><a href="https://raw.githubusercontent.com/pyviz/holoviews/master/examples/reference/elements/bokeh/Chord.ipynb">Bokeh</a></dd>
# MAGIC     <dd><a href="https://raw.githubusercontent.com/pyviz/holoviews/master/examples/reference/elements/matplotlib/Chord.ipynb">Matplotlib</a></dd>
# MAGIC </dl>
# MAGIC </div>

# COMMAND ----------

import pandas as pd
import holoviews as hv
from holoviews import opts, dim
from bokeh.sampledata.les_mis import data

hv.extension('bokeh')
hv.output(size=200)

# COMMAND ----------

# MAGIC %md The ``Chord`` element allows representing the inter-relationships between data points in a graph. The nodes are arranged radially around a circle with the relationships between the data points drawn as arcs (or chords) connecting the nodes. The number of chords is scaled by a weight declared as a value dimension on the ``Chord`` element.
# MAGIC
# MAGIC If the weight values are integers, they define the number of chords to be drawn between the source and target nodes directly. If the weights are floating point values, they are normalized to a default of 500 chords, which are divided up among the edges. Any non-zero weight will be assigned at least one chord.
# MAGIC
# MAGIC The ``Chord`` element is a type of ``Graph`` element and shares the same constructor. The most basic constructor accepts a columnar dataset of the source and target nodes and an optional value. Here we supply a dataframe containing the number of dialogues between characters of the *Les Misérables* musical. The data contains ``source`` and ``target`` node indices and an associated ``value`` column:

# COMMAND ----------

links = pd.DataFrame(data['links'])
print(links.head(3))

# COMMAND ----------

# MAGIC %md In the simplest case we can construct the ``Chord`` by passing it just the edges

# COMMAND ----------

hv.Chord(links)

# COMMAND ----------

# MAGIC %md The plot automatically adds hover and tap support, letting us reveal the connections of each node.
# MAGIC
# MAGIC To add node labels and other information we can construct a ``Dataset`` with a key dimension of node indices.

# COMMAND ----------

nodes = hv.Dataset(pd.DataFrame(data['nodes']), 'index')
nodes.data.head()

# COMMAND ----------

# MAGIC %md Additionally we can now color the nodes and edges by their index and add some labels. The ``labels``, ``node_color`` and ``edge_color`` options allow us to reference dimension values by name.

# COMMAND ----------

chord = hv.Chord((links, nodes)).select(value=(5, None))
chord.opts(
    opts.Chord(cmap='Category20', edge_cmap='Category20', edge_color=dim('source').str(), 
               labels='name', node_color=dim('index').str()))
