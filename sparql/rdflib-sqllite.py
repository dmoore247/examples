# Databricks notebook source
# MAGIC %md # Test rdflib-sqlalchemy

# COMMAND ----------

# MAGIC %pip install rdflib rdflib-sqlalchemy

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp
# MAGIC wget https://raw.githubusercontent.com/RDFLib/rdflib-sqlalchemy/develop/test/sp2b/1ktriples.n3

# COMMAND ----------

# MAGIC %sh ls /tmp/*.n3

# COMMAND ----------

# MAGIC %md ## Load Sqlite3 database with triples

# COMMAND ----------



# COMMAND ----------

from rdflib import plugin
from rdflib.graph import Graph
from rdflib.store import Store
from rdflib_sqlalchemy import registerplugins

registerplugins()

SQLALCHEMY_URL = 'sqlite:////tmp/triples.db'
TRIPLE_SOURCE  = "https://raw.githubusercontent.com/RDFLib/rdflib-sqlalchemy/develop/test/sp2b/1ktriples.n3"

store = plugin.get("SQLAlchemy", Store)(identifier="my_store")
graph = Graph(store, identifier="my_graph")
graph.open(SQLALCHEMY_URL, create=True)

graph.parse(TRIPLE_SOURCE, format="n3")
graph.commit()

result = graph.query("select * where {?s ?p ?o} limit 10")

for subject, predicate, object_ in result:
    print(subject, predicate, object_)

graph.close()

# COMMAND ----------

# DBTITLE 1,Re use existing store
store = plugin.get("SQLAlchemy", Store)(identifier="my_store")
graph = Graph(store, identifier="my_graph")
graph.open(SQLALCHEMY_URL, create=True)

result = graph.query("select * where {?s ?p ?o} limit 10")

for subject, predicate, object_ in result:
    print(subject, predicate, object_)

graph.close()

# COMMAND ----------

store = plugin.get("SQLAlchemy", Store)(identifier="my_store")
graph = Graph(store, identifier="my_graph")
graph.open(SQLALCHEMY_URL, create=True)

result = graph.query("select ?p (count(*) as ?c) where {?s ?p ?o} GROUP BY ?p ORDER BY DESC(COUNT(*))")

for p, c in result:
  print(c, p)

graph.close()

# COMMAND ----------

store = plugin.get("SQLAlchemy", Store)(identifier="my_store")
graph = Graph(store, identifier="my_graph")
graph.open(SQLALCHEMY_URL)

result = graph.query("SELECT ?s (COUNT(*) as ?c) where {?s ?p ?o} GROUP BY ?s ORDER BY DESC(COUNT(*))")

for p, c in result:
  print(c, p)

graph.close()

# COMMAND ----------


