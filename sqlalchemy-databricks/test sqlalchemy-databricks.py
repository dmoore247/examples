# Databricks notebook source
# MAGIC %pip install databricks-sql-connector==2.1.dev1666624638

# COMMAND ----------

access_token = "dapibb8eaf078822ba66b385cbb207ecdfd3"

# COMMAND ----------

from sqlalchemy import *
from sqlalchemy.engine import create_engine

host = "e2-demo-field-eng.cloud.databricks.com"
http_path = "sql/protocolv1/o/1444828305810485/1004-144036-yiqwahfn"
catalog = "hive_metastore"
schema = "douglas_moore_bronze"

SQLALCHEMY_URL = f"databricks+thrift://token:{access_token}@{host}/{schema}?http_path={http_path}&catalog={catalog}&schema={schema}"
engine = create_engine(SQLALCHEMY_URL)
conn = engine.connect()
conn.close()

# COMMAND ----------

from sqlalchemy import *
from sqlalchemy.engine import create_engine

#sql/protocolv1/o/1444828305810485/1004-144036-yiqwahfn
engine = create_engine(
    "databricks+connector://token:dapibb8eaf078822ba66b385cbb207ecdfd3@e2-demo-field-eng.cloud.databricks.com:443/douglas_moore_bronze",
    connect_args={
        "http_path": "sql/protocolv1/o/1444828305810485/1004-144036-yiqwahfn",
    },
)

logs = Table("iris", MetaData(bind=engine), autoload=True)
print(select([func.count("*")], from_obj=logs).scalar())

# COMMAND ----------

# MAGIC %sql show tables in douglas_moore_bronze

# COMMAND ----------

# MAGIC %pip install rdflib rdflib-sqlalchemy sqlalchemy-databricks databricks-sql-connector==2.1.dev1666624638

# COMMAND ----------

access_token = "dapibb8eaf078822ba66b385cbb207ecdfd3"

# COMMAND ----------

from rdflib import plugin
from rdflib.graph import Graph
from rdflib.store import Store
from rdflib_sqlalchemy import registerplugins

from sqlalchemy import *
from sqlalchemy.engine import create_engine

registerplugins()

#SQLALCHEMY_URL ="postgresql+psycopg2://user:password@hostname:port/databasename"
host = "e2-demo-field-eng.cloud.databricks.com"
http_path = "sql/protocolv1/o/1444828305810485/1004-144036-yiqwahfn"
catalog = "uc_demos_douglas_moore"
schema = "ekg_rdflib"

SQLALCHEMY_URL = f"databricks+thrift://token:{access_token}@{host}?http_path={http_path}&catalog={catalog}&schema={schema}"

store = plugin.get("SQLAlchemy", Store)(identifier="my_store")
graph = Graph(store, identifier="my_graph")
graph.open(SQLALCHEMY_URL, create=True)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC use douglas_moore_bronze;
# MAGIC describe kb_1f3fae7856_asserted_statements;

# COMMAND ----------

configuration

# COMMAND ----------

store.open(co)

# COMMAND ----------



graph.parse("https://raw.githubusercontent.com/RDFLib/rdflib-sqlalchemy/develop/test/sp2b/1ktriples.n3", format="n3")

result = graph.query("select * where {?s ?p ?o} limit 10")

for subject, predicate, object_ in result:
    print(subject, predicate, object_)

graph.close()

# COMMAND ----------


