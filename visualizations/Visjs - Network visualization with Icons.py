# Databricks notebook source
# MAGIC %md
# MAGIC # Network visualization using Pyvis + Vis.js + FontAwesome
# MAGIC
# MAGIC <table style='width="100%'>
# MAGIC   <tr><td>Vis.js</td><td>pyvis</td></tr>
# MAGIC   <tr><td>
# MAGIC <a href="https://visjs.github.io/vis-network/docs/network/index.html" />
# MAGIC <img src="https://visjs.org/images/visjs_logo.png">
# MAGIC </a>
# MAGIC   </td><td>
# MAGIC <img height="400" width="400" src="https://github.com/WestHealth/pyvis/raw/master/pyvis/source/tut.gif?raw=true">
# MAGIC </td>
# MAGIC </tr>
# MAGIC </table>
# MAGIC
# MAGIC ## Requires
# MAGIC * pyvis - Python wrapper around Vis.js
# MAGIC * Tested on DBR 6.2 ML beta

# COMMAND ----------

# DBTITLE 1,Game of Thrones network
from pyvis.network import Network
import pandas as pd

got_net = Network(height="750px", width="100%", bgcolor="#222222", font_color="white")

# set the physics layout of the network
got_net.barnes_hut()
got_data = pd.read_csv("https://www.macalester.edu/~abeverid/data/stormofswords.csv")

sources = got_data['Source']
targets = got_data['Target']
weights = got_data['Weight']

edge_data = zip(sources, targets, weights)

for e in edge_data:
    src = e[0]
    dst = e[1]
    w = e[2]

    got_net.add_node(src, src, title=src)
    got_net.add_node(dst, dst, title=dst)
    got_net.add_edge(src, dst, value=w)

neighbor_map = got_net.get_adj_list()

# add neighbor data to node hover data
for node in got_net.nodes:
    node["title"] += " Neighbors:<br>" + "<br>".join(neighbor_map[node["id"]])
    node["value"] = len(neighbor_map[node["id"]])

got_net.show("gameofthrones.html")

# COMMAND ----------

displayHTML(got_net.html)

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,DAG visualization example
from pyvis.network import Network

net = Network(height="600px", width="100%", bgcolor="#222222", font_color="white", notebook=True, directed=True)
#define 'users' and 'table' icon groups, codes from FontAwesome https://fontawesome.com/v4.7.0/
net.options.groups = {
            "users": {
                "shape": 'icon',
                "icon": {
                    "face": 'FontAwesome',
                    "code": '\uf0c0',
                    "size": 50,
                    "color": 'orange'
                }
            },
            "table": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf0ce',
                  "size": 50,
                  "color": 'green'
                }
            },
              "database": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf1c0',
                  "size": 50,
                  "color": 'green'
                }
            },
              "notebook": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf02d',
                  "size": 50,
                  "color": 'red'
                }
            },
              "account": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf0f7',
                  "size": 50,
                  "color": 'yellow'
                }
            },
              "AE": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf007',
                  "size": 50,
                  "color": 'yellow'
                }
            },
              "job": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf073',
                  "size": 50,
                  "color": 'red'
                }
            },
              "cluster": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf0e8',
                  "size": 50,
                  "color": 'red'
                }
            },
              "workspace": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf07c',
                  "size": 50,
                  "color": 'red'
                }
            },
              "subscription": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf0c2',
                  "size": 50,
                  "color": 'aqua'
                }
            },
              "domain": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf0ac',
                  "size": 50,
                  "color": 'orange'
                }
            },
              "user": {                 
              "shape": 'icon',
              "icon": {
                  "face": 'FontAwesome',
                  "code": '\uf007',
                  "size": 50,
                  "color": 'orange'
                }
            }
        }
# define graph
net.add_node(1, label="analysts", shape="icon", group="users")
net.add_node(2, label="table", shape="icon", group="table")
net.add_node(3, label="database", shape="icon", group="database")
net.add_node(4, label="notebook", shape="icon", group="notebook")
#net.add_node(5, label="workspace", shape="image", image="https://docs.microsoft.com/en-us/azure/architecture/_images/icons/databricks.png")
net.add_node(5, label="workspace", shape="icon", group="workspace")
net.add_node(6, label="account", shape="icon", group="account")
net.add_node(7, label="AE", shape="icon", group="AE")
net.add_node(8, label="job", shape="icon", group="job")
net.add_node(9, label="cluster", shape="icon", group="cluster")
net.add_node(10, label="subscription", shape="icon", group="subscription")
net.add_node(11, label="domain", shape="icon", group="domain")
net.add_node(12, label="user", shape="icon", group="user")

net.add_edge(1,4,label="uses")
net.add_edge(3,2,label="contains")
net.add_edge(4,2, label="queries")
net.add_edge(5,4, label="contains")
net.add_edge(5,6, label="belongs")
net.add_edge(7,6, label="owns")
net.add_edge(8,4, label="runs")
net.add_edge(5,9, label="manages")
net.add_edge(4,9, label="attached")
net.add_edge(10,5, label="contains")
net.add_edge(12,9, label="creates")
net.add_edge(12,11, label="has")
net.add_edge(11,6, label="linked")
net.add_edge(12,4, label="ran")
net.show("net.html")

# patch in font-awesome css https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css
html_str = net.html.replace(
  '<head>',
  '<head><link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css" type="text/css"/>'
)
#print(html_str)
displayHTML(html_str)
