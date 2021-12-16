# Databricks notebook source
# MAGIC %md # CloudTrail logs data multiplier
# MAGIC  
# MAGIC 
# MAGIC [AWS CloudTrail](https://aws.amazon.com/cloudtrail/) is a web service that records AWS API calls for your account and delivers audit logs to you as JSON files in a S3 bucket. If you do not have it configured, see AWS' documentation on how to do so. 
# MAGIC 
# MAGIC This notebook multiplies and generates new synthetic data for CloudTrail
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
# MAGIC     "autoscale": {
# MAGIC         "min_workers": 2,
# MAGIC         "max_workers": 30
# MAGIC     },
# MAGIC     "node_type_id": "c5d.2xlarge",
# MAGIC     "driver_node_type_id": "r4.4xlarge",
# MAGIC     "spark_version": "8.3.x-scala2.12",
# MAGIC     "spark_conf": {
# MAGIC         "spark.driver.maxResultSize": "20GB"
# MAGIC     },
# MAGIC     ```

# COMMAND ----------

# MAGIC %md ## Periodic job to ingest cloud trail into Delta Lake for analysis
# MAGIC - Using Trigger.Once, this job will run and ingest all 'new' files

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

import time
files = dbutils.fs.ls(raw_path)

if reset:
  dbutils.fs.rm(input_path, recurse=True)
  dbutils.fs.mkdirs(input_path)
  index = 1

# COMMAND ----------

# DBTITLE 1,Data Generator code (runs in background thread)
def do_copies(index = 1):
  for i in range(n_copies):
    print(F"""Iteration {i}/{n_copies}""")
    for file_info in files:
      f = file_info.name
      src = F"""{raw_path}/{f}"""
      dst = F"""{input_path}/{f}_{index}.json.gz"""
      print(F"""from {src} to {dst}""")
      dbutils.fs.cp(src,dst)

      print(F"Sleeping {wait_s} seconds")
      #time.sleep(wait_s)
      time.sleep(wait_s)
      index = index + 1


import threading

# do some stuff
background_thread = threading.Thread(target=do_copies, name="Data gen")
background_thread.start()
# continue doing stuff

# COMMAND ----------

background_thread.is_alive()

# COMMAND ----------

for i in range (100):
  display(dbutils.fs.ls(input_path))
  time.sleep(20)

# COMMAND ----------

df = spark.read.format('binaryFile').load(input_path)
schema = df.schema

# COMMAND ----------

df = spark.readStream.format('binaryFile').schema(schema).load(input_path).drop('content')
df.createOrReplaceTempView('cloud_files_raw')

# COMMAND ----------

# MAGIC %sql select * from cloud_files_raw

# COMMAND ----------

df_output = spark.readStream.format('delta').table('db_audit.cloudtrail_bronze_3')
df_output.createOrReplaceTempView('cloud_trail_stream')

# COMMAND ----------

# MAGIC %sql
# MAGIC select timestamp, eventName, count(*) cnt 
# MAGIC from cloud_trail_stream 
# MAGIC group by 1,2

# COMMAND ----------


