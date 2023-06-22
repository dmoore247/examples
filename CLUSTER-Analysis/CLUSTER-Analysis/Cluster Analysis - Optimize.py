# Databricks notebook source
# MAGIC %md # Optimize

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.widgets.text("schema_name", defaultValue="main.douglas_moore", label="Target Schema")
# MAGIC schema_name = dbutils.widgets.get("schema_name")
# MAGIC spark.conf.set('c.schema_name',schema_name)
# MAGIC
# MAGIC schema_name, spark.conf.get('c.schema_name')

# COMMAND ----------

spark.sql(f"OPTIMIZE {schema_name}.bronze_clusters ZORDER BY (cluster_id)").display()

# COMMAND ----------

spark.sql(f"OPTIMIZE {schema_name}.bronze_cluster_events ZORDER BY (cluster_id,timestamp)").display()

# COMMAND ----------


