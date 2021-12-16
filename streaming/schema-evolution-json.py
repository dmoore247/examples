# Databricks notebook source
# MAGIC %fs ls s3a://oetrta/dmoore

# COMMAND ----------

# MAGIC %fs mkdirs s3a://oetrta/dmoore/landing/schema-evolve

# COMMAND ----------

landing_zone = "s3a://oetrta/dmoore/landing/schema-evolve/"
input_format = "json"
output_path = "s3a://oetrta/dmoore/bronze/schema-evolve/"
schema_path = "s3a://oetrta/dmoore/schemas/schema-evolve/"
checkpoint_path = "s3a://oetrta/dmoore/checkpoints/schema-evolve/"
badrecords_path = "s3a://oetrta/dmoore/badrecords/schema-evolve"
includeExistingFiles = False
table_name = 'douglas_moore_bronze.schema_evolve_target'

# COMMAND ----------

def run_data_reset():
  dirs = [landing_zone, output_path, schema_path, checkpoint_path, badrecords_path]
  for _path in dirs:
    print(F"Cleaning {_path}")
    dbutils.fs.rm(_path, recurse=True)
    dbutils.fs.mkdirs(_path)

# COMMAND ----------

run_data_reset()

# COMMAND ----------

import json
import time

event_1 = {
  'message_id': '12345abcd',
  'payload': '''This is a big payload 1''',
  'col1':1234.567
}

event_2 = {
  'message_id': '98765642',
  'payload': '''This is a big payload 2''',
  'col2':567.12
}

event_3 = {
  'message_id': 'zedfa57283',
  'payload': '''This is a big payload 3''',
  'col3':9684.2
}

event_4 = {
  'message_id': 'zedfa572666',
  'payload': '''This is a big payload 4''',
  'col4': "0000.1"
}

def run_datagen(wait=20):
  dbutils.fs.rm(landing_zone, recurse=True)
  dbutils.fs.mkdirs(landing_zone)
  dbutils.fs.put(landing_zone+'1.json',json.dumps(event_1))
  time.sleep(wait)
  dbutils.fs.put(landing_zone+'2.json',json.dumps(event_2))
  time.sleep(wait)
  dbutils.fs.put(landing_zone+'3.json',json.dumps(event_3))
  time.sleep(wait)
  dbutils.fs.put(landing_zone+'4.json',json.dumps(event_4))

# COMMAND ----------

input_format, schema_path, includeExistingFiles, landing_zone, badrecords_path

# COMMAND ----------

run_datagen(0)

# COMMAND ----------

spark.conf.set("spark.databricks.cloudFiles.schemaInference.sampleSize.numFiles", 3)
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", input_format)
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaHints", "message_id string, payload string, col1 double")
    .option("cloudFiles.inferColumnTypes","true")
    .option("badRecordsPath", badrecords_path)
    .option("rescuedDataColumn", "_rescue")
    .option("includeExistingFiles","true")
    .option("maxFilesPerTrigger", "1")
    .load(landing_zone)
     )

# COMMAND ----------

dbutils.fs.ls(badrecords_path)

# COMMAND ----------

df.display()

# COMMAND ----------

q = (
  df.writeStream
    .format('delta')
    .trigger(once=True)
    .option('checkpointLocation',checkpoint_path)
    .option('mergeSchema','true')
    .table(table_name)
)
q.status

# COMMAND ----------

spark.read.table(table_name).display()

# COMMAND ----------

# MAGIC %sql delete from douglas_moore_bronze.schema_evolve_target

# COMMAND ----------

# write
q = (df.writeStream
       .format('delta')
       .option('checkpointLocation',checkpoint_path)
       .option('mergeSchema','true')
       .trigger(processingTime='5 seconds')
       .table(table_name)
    )

# COMMAND ----------

# MAGIC %fs ls s3a://oetrta/dmoore/badrecords/schema-evolve/

# COMMAND ----------

spark.readStream.table(table_name).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history ${table_name}

# COMMAND ----------

run_datagen()

# COMMAND ----------

spark.readStream.table(table_name).createOrReplaceTempView('target_schema_evolve')
display(spark.sql('select * from target_schema_evolve'))

# COMMAND ----------

# MAGIC %sql select * from douglas_moore_bronze.schema_evolve_target

# COMMAND ----------

# MAGIC %sql select * from target_schema_evolve

# COMMAND ----------

while q.isActive:
  print(q.status)
  time.sleep(5)

# COMMAND ----------

#run_datagen()

# COMMAND ----------

df = spark.readStream.format('rate').option("rowsPerSecond", 10).load()

# COMMAND ----------

df.withColumn('')

# COMMAND ----------

df.display()

# COMMAND ----------


