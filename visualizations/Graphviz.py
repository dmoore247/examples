# Databricks notebook source
# MAGIC %md # Displaying graphviz
# MAGIC Graphviz renders Dot notation into a visual network graph.
# MAGIC
# MAGIC ![](https://camo.githubusercontent.com/442bd05eb849b2f3de1c3abe8ce186afd0bc02e6/68747470733a2f2f7261772e6769746875622e636f6d2f78666c72362f677261706876697a2f6d61737465722f646f63732f726f756e642d7461626c652e706e67)
# MAGIC
# MAGIC You can display Graphviz visuals inline to your Databricks notebooks
# MAGIC
# MAGIC ## Requirements
# MAGIC * Install Dot `sudo apt-get install graphviz -y`
# MAGIC * Install python library `graphviz`
# MAGIC * Render visual using SVG

# COMMAND ----------

# MAGIC %sh sudo apt-get install graphviz -y

# COMMAND ----------

# MAGIC %pip install graphviz

# COMMAND ----------

ml_cluster = True
if (not ml_cluster):
  dbutils.library.installPyPI('graphviz')
  dbutils.library.restartPython()

# COMMAND ----------

from graphviz import Digraph

dot = Digraph(comment='The Round Table', format='svg')

dot.node('A', 'King Arthur')
dot.node('B', 'Sir Bedevere the Wise')
dot.node('L', 'Sir Lancelot the Brave')

dot.edges(['AB', 'AL'])
dot.edge('B', 'L', constraint='false')

#dot.save('/dbfs/FileStore/dmoore/graphviz-plot-2.svg')

# COMMAND ----------

displayHTML(dot.pipe().decode('utf-8'))

# COMMAND ----------

from graphviz import Digraph
dot = Digraph(comment='Data flow', format='svg')
dot.attr(rankdir='LR')
dot.node('A','JSON',shape="folder")
dot.node('B','Spark')
dot.node('C','Kakfa', shape="box")
dot.node('D','Delta[Bronze]', shape="folder")
dot.node('E','Delta[Silver]', shape="folder")
dot.node('S','Spark')
dot.node('T','Spark')
dot.edges(['AB', 'BC', 'CS', 'SD', 'DT','TE'])

html=dot.pipe().decode('utf-8')
#print(html)
displayHTML(html)

