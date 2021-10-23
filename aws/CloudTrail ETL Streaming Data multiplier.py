# Databricks notebook source
# MAGIC %md # CloudTrail logs data multiplier
# MAGIC  
# MAGIC 
# MAGIC [AWS CloudTrail](https://aws.amazon.com/cloudtrail/) is a web service that records AWS API calls for your account and delivers audit logs to you as JSON files in a S3 bucket. If you do not have it configured, see AWS' documentation on how to do so. 
# MAGIC 
# MAGIC This job uses AutoLoader to track what's been loaded: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html
# MAGIC simplifying the ETL logic considerably.
# MAGIC 
# MAGIC 
# MAGIC Author: Douglas Moore
# MAGIC 
# MAGIC Tags: CloudTrail, Autoloader, cloudFiles, JSON, Historical Load, Incremental Load, Continious, Streaming, Production, Widgets, Job, Ingest

# COMMAND ----------

# MAGIC %md ## Configuration Documentation
# MAGIC | num | Input Field | Description |
# MAGIC | --- | --- | --- |
# MAGIC |00. | CloudTrail Raw Data    | < path to raw data to multiply into CloudTrail input |       
# MAGIC |01. | CloudTrail Input       | < path to log files. Glob patterns supported > |
# MAGIC |02. | # Copies               | < number of copies to make > |
# MAGIC |03. | Wait (Seconds)         | < number of seconds between each file generated |
# MAGIC |04. | Reset                  | True == Reset Input folder |

# COMMAND ----------

# MAGIC %md ## Requires
# MAGIC - DBR 8.3
# MAGIC - Spark config:
# MAGIC `spark.driver.maxResultSize 20GB`
# MAGIC - Cluster config:
# MAGIC ```
# MAGIC     "num_workers": 4,
# MAGIC     "spark_version": "8.3.x-scala2.12",
# MAGIC     "spark_conf": {
# MAGIC         "spark.databricks.io.cache.maxMetaDataCache": "1g",
# MAGIC         "spark.databricks.io.cache.maxDiskUsage": "50g",
# MAGIC         "spark.databricks.io.cache.compression.enabled": "false",
# MAGIC         "spark.driver.maxResultSize": "20GB",
# MAGIC         "spark.databricks.io.cache.enabled": "true"
# MAGIC     },
# MAGIC     "node_type_id": "c5d.2xlarge",
# MAGIC     "driver_node_type_id": "r5.4xlarge",
# MAGIC     ```

# COMMAND ----------

# DBTITLE 1,Process Input Parameters
dbutils.widgets.removeAll()
dbutils.widgets.text("raw_path",defaultValue="s3a://oetrta/dmoore/flaws_cloudtrail_logs", label="00 CloudTrail raw data")
dbutils.widgets.text("input_path",defaultValue="s3a://oetrta/dmoore/flaws_cloudtrail_landing", label="01 CloudTrail Input")
dbutils.widgets.text("n_copies",defaultValue="50", label="02 # copies")
dbutils.widgets.text("wait_s",defaultValue="5", label="03 # Wait (seconds)")
dbutils.widgets.combobox("reset",defaultValue="true", choices=["false","true"], label="04 Reset")

raw_path = dbutils.widgets.get("raw_path")
input_path = dbutils.widgets.get("input_path")

n_copies = int(dbutils.widgets.get("n_copies"))
wait_s = int(dbutils.widgets.get("wait_s"))
reset  = dbutils.widgets.get("reset")

print(F"""
raw_path        {raw_path}, 
input_path      {input_path},
n_copies        {n_copies},
wait_s          {wait_s},
reset           {reset}

""")

# COMMAND ----------

dbutils.fs.mkdirs(input_path)

# COMMAND ----------

# MAGIC %md ## Monitor and inject test data

# COMMAND ----------

# DBTITLE 1,Streaming file list
schema = spark.read.format("binaryFile").option("path",input_path).load().schema
df = spark.readStream.format("binaryFile").option("path",input_path).schema(schema).load().drop('content')
display(df)

# COMMAND ----------

# DBTITLE 1,Reset
import time
files = dbutils.fs.ls(raw_path)

if True:
  dbutils.fs.rm(input_path, recurse=True)
  dbutils.fs.mkdirs(input_path)
  index = 1

# COMMAND ----------

# DBTITLE 1,Data Generator code (runs in background thread)
import uuid
stop_threads = False
def do_copies(index = 1):
  for i in range(n_copies):
    print(F"""Iteration {i}/{n_copies}""")
    for file_info in files:
      f = file_info.name
      src = F"""{raw_path}/{f}"""
      dst = F"""{input_path}/{f}_{index}_{str(uuid.uuid4())}.json.gz"""
      print(F"""from {src} to {dst}""")
      dbutils.fs.cp(src,dst)

      print(F"Sleeping {wait_s} seconds")
      #time.sleep(wait_s)
      time.sleep(wait_s)
      index = index + 1
      global stop_threads
      if stop_threads:
        return

import threading

# do some stuff
background_thread = threading.Thread(target=do_copies, name="Data gen")
background_thread.start()
# continue doing stuff
print("\n running")
background_thread.is_alive()

import time
time.sleep(10)

# COMMAND ----------

display(dbutils.fs.ls(input_path))

# COMMAND ----------

# DBTITLE 1,Stop notebook in-case of run-all
dbutils.notebook.exit(0)

# COMMAND ----------

# DBTITLE 1,Stop background thread
background_thread.is_alive()
stop_threads = True

# COMMAND ----------

(dbutils.fs.rm(input_path, recurse=True))

# COMMAND ----------


