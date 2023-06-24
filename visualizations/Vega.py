# Databricks notebook source
# MAGIC %md # Vega barchart

# COMMAND ----------

# DBTITLE 1,Define Helper Functions
import json
def displayVega(spec):
  displayHTML(
  """
<html>
<head>
  <title>Embedding Vega-Lite</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vega/3.0.0/vega.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vega-lite/2.0.0-beta.14/vega-lite.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/vega-embed/3.0.0-beta.20/vega-embed.js"></script>
</head>
<body>

  <div id="vis"></div>

  <script type="text/javascript">
    var yourVlSpec = {spec}
    vega.embed("#vis", yourVlSpec);
  </script>
</body>
</html>
""".format(spec=spec)
)
  

# COMMAND ----------

# DBTITLE 1,Data and Vega Spec
values = [
          {"a": "A","b": 28}, 
          {"a": "B","b": 55}, 
          {"a": "C","b": 43},
          {"a": "D","b": 91}, 
          {"a": "E","b": 81}, 
          {"a": "F","b": 53},
          {"a": "G","b": 19}, 
          {"a": "H","b": 87}, 
          {"a": "I","b": 52}
 ]

spec = {
      "$schema": "https://vega.github.io/schema/vega-lite/v2.0.json",
      "description": "A simple bar chart with embedded data.",
      "data":  {
        "values": values
      },
      "mark": "bar",
      "encoding": {
        "x": {"field": "a", "type": "ordinal"},
        "y": {"field": "b", "type": "quantitative"}
      }
    }
displayVega(json.dumps(spec))

# COMMAND ----------

# MAGIC %md # Sunburst Vega Demo
# MAGIC * Load data using Python's requests library
# MAGIC * Embed values read into Vega spec

# COMMAND ----------

import requests
url = "https://raw.githubusercontent.com/vega/vega/master/packages/vega/test/data/flare.json"
r = requests.get(url)
values = r.json()
#print (values)

# COMMAND ----------

# DBTITLE 1,Sunburst Spec
#       "url": "https://raw.githubusercontent.com/vega/vega/master/packages/vega/test/data/flare.json",
spec = {
  "$schema": "https://vega.github.io/schema/vega/v5.json",
  "width": 600,
  "height": 600,
  "padding": 5,
  "autosize": "none",

  "data": [
    {
      "name": "tree",
      "values": values,
      "transform": [
        {
          "type": "stratify",
          "key": "id",
          "parentKey": "parent"
        },
        {
          "type": "partition",
          "field": "size",
          "sort": {"field": "value"},
          "size": [{"signal": "2 * PI"}, {"signal": "width / 2"}],
          "as": ["a0", "r0", "a1", "r1", "depth", "children"]
        }
      ]
    }
  ],

  "scales": [
    {
      "name": "color",
      "type": "ordinal",
      "domain": {"data": "tree", "field": "depth"},
      "range": {"scheme": "tableau20"}
    }
  ],

  "marks": [
    {
      "type": "arc",
      "from": {"data": "tree"},
      "encode": {
        "enter": {
          "x": {"signal": "width / 2"},
          "y": {"signal": "height / 2"},
          "fill": {"scale": "color", "field": "depth"},
          "tooltip": {"signal": "datum.name + (datum.size ? ', ' + datum.size + ' bytes' : '')"}
        },
        "update": {
          "startAngle": {"field": "a0"},
          "endAngle": {"field": "a1"},
          "innerRadius": {"field": "r0"},
          "outerRadius": {"field": "r1"},
          "stroke": {"value": "white"},
          "strokeWidth": {"value": 0.5},
          "zindex": {"value": 0}
        },
        "hover": {
          "stroke": {"value": "red"},
          "strokeWidth": {"value": 2},
          "zindex": {"value": 1}
        }
      }
    }
  ]
}

displayVega(json.dumps(spec))

# COMMAND ----------


