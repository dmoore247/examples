# Databricks notebook source
# MAGIC %pip install kglab

# COMMAND ----------

import kglab

kg = kglab.KnowledgeGraph().load_rdf("tmp.ttl")

# COMMAND ----------

VIS_STYLE = {
    "wtm": {
        "color": "orange",
        "size": 40,
    },
    "ind":{
        "color": "blue",
        "size": 30,
    },
    "skos":{
      "color": "red",
      "size": 30,
    },
    "nom":{
      "color":"green",
      "size":30,
    },
    "wd":{
      "color":"navy",
      "size":30,
    }
}

subgraph = kglab.SubgraphTensor(kg)
pyvis_graph = subgraph.build_pyvis_graph(notebook=False, style=VIS_STYLE)
#pyvis_graph.force_atlas_2based()
displayHTML(pyvis_graph.generate_html())

# COMMAND ----------

pyvis_graph.nodes
