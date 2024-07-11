# Databricks notebook source
# MAGIC %md # Extract data from clusters API for operational analytics

# COMMAND ----------

# MAGIC %md ## Setup and configuration

# COMMAND ----------

# MAGIC %pip install --quiet databricks-sdk==0.9.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import expr, lit
from databricks.sdk import WorkspaceClient
import pprint
pp = pprint.PrettyPrinter(indent=4)

# COMMAND ----------

#dbutils.widgets.removeAll()
dbutils.widgets.text("profile", defaultValue="E2DEMO", label="Databricks Environment Profile")
dbutils.widgets.text("scope", defaultValue="tokens", label="Secret Scope")
dbutils.widgets.text("schema_name", defaultValue="main.douglas_moore", label="Target Schema")

profile = dbutils.widgets.get("profile")
scope = dbutils.widgets.get("scope")
schema_name = dbutils.widgets.get("schema_name")
spark.conf.set('c.schema_name',schema_name)

# Setup databricks connection config file. Pull from the secrets store
open(".databrickscfg","w").write(dbutils.secrets.get(scope=scope,key='databrickscfg'))

profile, scope, schema_name, spark.conf.get('c.schema_name')

# COMMAND ----------

# MAGIC %md ## Make API calls to collect Cluster information

# COMMAND ----------

client = WorkspaceClient(config_file=".databrickscfg", profile=profile)
host = client.config.host
assert host is not None
assert host[:5] == 'https'
host

# COMMAND ----------

clusters = client.clusters.list()
print(f"host: {host}, \nclusters: {len(clusters)}")

# COMMAND ----------

import os
try:
    os.remove(".databrickscfg")
except Exception as e:
    print(e)

# COMMAND ----------

clusters_ = []
cluster_events_ = []
for cluster in clusters:
    print(cluster.cluster_name, cluster.cluster_id)
    cluster_dict = cluster.as_dict()
    clusters_.append(cluster_dict)
    # event log
    events = client.clusters.events(cluster_id=cluster.cluster_id)
    for i,e in enumerate(events):
        cluster_events_.append(e.as_dict())
        if i > 1000:
            break

# COMMAND ----------

schema_string = """cluster_id STRING NOT NULL,
  cluster_name STRING,
  autoscale MAP<STRING, BIGINT>,
  autotermination_minutes BIGINT,
  aws_attributes MAP<STRING, STRING>,
  cluster_cores DOUBLE, 
  cluster_memory_mb BIGINT,
  cluster_source STRING,
  creator_user_name STRING,
  custom_tags MAP<STRING, STRING>,
  data_security_mode STRING,
  default_tags MAP<STRING, STRING>,
  driver MAP<STRING, STRING>,
  driver_node_type_id STRING,
  enable_elastic_disk BOOLEAN,
  enable_local_disk_encryption BOOLEAN,
  executors ARRAY<MAP<STRING, STRING>>,
  init_scripts ARRAY<MAP<STRING, MAP<STRING, STRING>>>,
  jdbc_port BIGINT,
  last_restarted_time BIGINT,
  last_state_loss_time BIGINT,
  node_type_id STRING,
  runtime_engine STRING,
  spark_conf MAP<STRING, STRING>,
  spark_context_id BIGINT,
  spark_version STRING,
  start_time BIGINT,
  state STRING,
  state_message STRING,
  policy_id STRING,
  cluster_log_conf MAP<STRING, MAP<STRING, STRING>>,
  cluster_log_status MAP<STRING, STRING>,
  num_workers BIGINT,
  spark_env_vars MAP<STRING, STRING>,
  single_user_name STRING,
  terminated_time BIGINT,
  termination_reason MAP<STRING, STRING>,
  driver_instance_pool_id STRING,
  instance_pool_id STRING"""

# COMMAND ----------

clusters_df = spark.createDataFrame(clusters_,schema_string)

# COMMAND ----------

display(clusters_df)

# COMMAND ----------

events_df = spark.createDataFrame(cluster_events_,schema='struct<cluster_id:string,timestamp:bigint,type:string,details:map<string,string>>')

# COMMAND ----------

display(events_df.orderBy(['cluster_id','timestamp']))

# COMMAND ----------

# MAGIC %md ## Save Clusters & Cluster Events API results

# COMMAND ----------

clusters_save_df = (clusters_df
                    .withColumn("host", lit(host))
                    .withColumn("load_dt",expr('now()'))
                    )

# COMMAND ----------

clusters_save_df.write.saveAsTable(f"{schema_name}.bronze_clusters",mode="append",mergeSchema="true")

# COMMAND ----------

events_save_df = (events_df
                    .withColumn("host", lit(host))
                    .withColumn("load_dt",expr('now()'))
                    )

# COMMAND ----------

events_save_df.write.saveAsTable(f"{schema_name}.bronze_cluster_events",mode="append",mergeSchema="true")

# COMMAND ----------

# MAGIC %md ## QA Bronze data

# COMMAND ----------

# MAGIC %sql select count(distinct cluster_id) from ${c.schema_name}.bronze_clusters

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(distinct cluster_id), count(1) 
# MAGIC from ${c.schema_name}.bronze_cluster_events

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * 
# MAGIC from ${c.schema_name}.bronze_cluster_events

# COMMAND ----------

# MAGIC %md # Finished
