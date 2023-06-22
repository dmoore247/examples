# Databricks notebook source
# MAGIC %md # Ingest workflow data files

# COMMAND ----------

# MAGIC %sql use hive_metastore.cto_ustst_alyt_eds

# COMMAND ----------

# MAGIC %sql drop table hive_metastore.default.priority_workflows 

# COMMAND ----------

# MAGIC %sql select current_catalog(), current_database()

# COMMAND ----------

path = "/Workspace/UAMS_DBX_ADMINS/CLUSTER Analysis/priority_workflow_list.txt"

# COMMAND ----------

workflows = open(path,'r').read().split('\n')
workflows

# COMMAND ----------

from pyspark.sql.types import StringType
priority_df = spark.createDataFrame(workflows,StringType())
priority_df.write.saveAsTable('priority_workflows', mode='overwrite')

# COMMAND ----------

path = "/Workspace/UAMS_DBX_ADMINS/CLUSTER Analysis/workflow_list.txt"

# COMMAND ----------

workflows = open(path,'r').read().split('\n')
workflows

# COMMAND ----------

from pyspark.sql.types import StringType

wf_df = spark.createDataFrame(workflows, StringType())

# COMMAND ----------

display(wf_df)

# COMMAND ----------

wf_df.write.saveAsTable('workflows', mode='overwrite')

# COMMAND ----------


