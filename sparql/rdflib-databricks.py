# Databricks notebook source
# MAGIC %md ## Test rdflib-sqlalchemy

# COMMAND ----------



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

from rdflib import plugin
from rdflib.graph import Graph
from rdflib.store import Store
from rdflib_sqlalchemy import registerplugins

registerplugins()

#SQLALCHEMY_URL ="postgresql+psycopg2://user:password@hostname:port/databasename"
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

# MAGIC %md # RDFLib on Databricks

# COMMAND ----------

# MAGIC %pip install databricks-sql-connector rdflib rdflib-sqlalchemy

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql create database if not exists hive_metastore.dm_rdflib_ekg;

# COMMAND ----------

from rdflib import plugin
from rdflib.graph import Graph
from rdflib.store import Store
from rdflib_sqlalchemy import registerplugins

from sqlalchemy import *
from sqlalchemy.engine import create_engine

configuration = {
  "url":"databricks://token:dapi4b31bf12ae7aa4159e603faeb9eb87db@e2-demo-field-eng.cloud.databricks.com:443/?http_path=/sql/1.0/warehouses/ead10bf07050390f&catalog=uc_demos_douglas_moore&schema=rdflib_ekg"
}
print(configuration)

registerplugins()

TRIPLE_SOURCE  = "https://raw.githubusercontent.com/RDFLib/rdflib-sqlalchemy/develop/test/sp2b/1ktriples.n3"

store = plugin.get("SQLAlchemy", Store)(identifier="my_store")
graph = Graph(store, identifier="my_graph")

graph.open(configuration, create=True)

graph.parse(TRIPLE_SOURCE, format="n3")
graph.commit()

result = graph.query("select * where {?s ?p ?o} limit 10")

for subject, predicate, object_ in result:
    print(subject, predicate, object_)

graph.close()

# COMMAND ----------

show tables in hive_metastore.
