# Databricks notebook source
# MAGIC %md # Historical, Incremental or Continious ingest and processing of CloudTrail logs
# MAGIC  
# MAGIC 
# MAGIC [AWS CloudTrail](https://aws.amazon.com/cloudtrail/) is a web service that records AWS API calls for your account and delivers audit logs to you as JSON files in a S3 bucket. If you do not have it configured, see AWS' documentation on how to do so. 
# MAGIC 
# MAGIC This job uses AutoLoader to track what's been loaded: https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html
# MAGIC simplifying the ETL logic considerably.
# MAGIC 
# MAGIC This job has 3 modes:
# MAGIC 1. Historical Load - Load millions of files collected over time before this was installed into the Delta table.
# MAGIC 2. Incremental Load - Run periodically (quarterly, monthly, weekly, daily, hourly, every 30 minutes...) to ingest the data into the delta table.
# MAGIC 3. Continious load - Run continiously ingesting data into the delta table.
# MAGIC 
# MAGIC Test data found here: https://summitroute.com/blog/2020/10/09/public_dataset_of_cloudtrail_logs_from_flaws_cloud/
# MAGIC 
# MAGIC Author: Douglas Moore
# MAGIC 
# MAGIC Tags: CloudTrail, Autoloader, cloudFiles, JSON, Historical Load, Incremental Load, Continious, Streaming, Production, Widgets, Job, Ingest

# COMMAND ----------

# MAGIC %md ## Configuration Documentation
# MAGIC | num | Input Field | Description |
# MAGIC | --- | --- | --- |
# MAGIC |01. | CloudTrail Input       | < path to log files. Glob patterns supported > |
# MAGIC |02. | Delta Table Path       | < path to delta table storage location > |
# MAGIC |03. | Checkpoint Path        | < path to checkpoint folder (tracks which files have been processed) |
# MAGIC |04. | Database               | Database name where table will be located |
# MAGIC |05. | Table                  | Table name for Delta Lake table
# MAGIC |06. | Load type              | Historical (one time load of all past data), Incremental (hourly, daily, weekly, quarterly runs), Continuous (always on)|

# COMMAND ----------

# MAGIC %md ## Requires
# MAGIC - DBR 8.3 +
# MAGIC - Spark config: (Large number of files creates pressure on the driver)
# MAGIC `spark.driver.maxResultSize 20GB`
# MAGIC - Cluster config:
# MAGIC   - Compute intenstive workers
# MAGIC   - Memory intensive driver
# MAGIC 
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
dbutils.widgets.text("input_path",defaultValue="s3a://oetrta/dmoore/flaws_cloudtrail_logs", label="01 CloudTrail Input")
dbutils.widgets.text("output_path", defaultValue="s3a://oetrta/dmoore/databases/db_audit/cloudtrail_bronze/", label="02 Delta Table Path")
dbutils.widgets.text("checkpoint_path", defaultValue="s3://oetrta/dmoore/chkpts/db_audit/cloudtrail_bronze/", label="03 Checkpoint path")
dbutils.widgets.text("database_name", defaultValue="db_audit", label="04 Database")
dbutils.widgets.text("table_name", defaultValue="cloudtrail_bronze", label="05 Table")

dbutils.widgets.combobox("load_type",defaultValue="incremental", choices=["historical","incremental","continuous"],label="06 Load type")
#dbutils.widgets.combobox("mergeSchema",defaultValue="false", choices=["true","false"],label="07 Merge Schema")
#dbutils.widgets.combobox("maxFilesPerTrigger",defaultValue="10,000", choices=["100,000", "10,000", "1,000", "10", "1"],label="08 maxFilesPerTrigger")
#dbutils.widgets.combobox("maxBytesPerTrigger",defaultValue="100m", choices=["1m","10m","100m","1g","10g","100g", "1000g"],label="08 maxBytesPerTrigger")

#dbutils.widgets.combobox("includeExistingFiles", defaultValue="false", choices=["true","false"], label="09 Include existing files")
#dbutils.widgets.combobox("mode", defaultValue="append", choices=["append","overwrite"], label="10 Write mode")
#dbutils.widgets.combobox("create_table", defaultValue="false", choices=["true","false"], label="11 Create table")

input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")  # DBFS or S3 path 
checkpoint_path = dbutils.widgets.get("checkpoint_path")
database_name = dbutils.widgets.get("database_name")
table_name = dbutils.widgets.get("table_name")
database_table_name = F"{database_name}.{table_name}"
load_type = dbutils.widgets.get("load_type")

options = {
  "historical": {
    "mergeSchema": False,
    "maxFilesPerTrigger": 10000,
    "includeExistingFiles": True,
    "mode": "append",
    "create_database": True,
    "create_table": True,
    "remove_checkpoint": True,
    "trigger": { "type": "once", "value":"True" },
  },
  "incremental": {
    "mergeSchema": True,
    "maxFilesPerTrigger": 10000,
    "includeExistingFiles": False,
    "mode": "append",
    "create_database": False,
    "create_table": False,
    "remove_checkpoint": False,
    "trigger": { "type": "once", "value":"True" }
  },
  "continuous": {
    "mergeSchema": True,
    "maxFilesPerTrigger": 1000,
    "includeExistingFiles": False,
    "mode": "append",
    "create_database": False,
    "create_table": False,
    "remove_checkpoint": False,
    "trigger": { "type": "processing", "value":"60 seconds" }
  }
}

mergeSchema = options[load_type]['mergeSchema']
maxFilesPerTrigger = options[load_type]['maxFilesPerTrigger']
includeExistingFiles = options[load_type]['includeExistingFiles']
mode = options[load_type]['mode']
remove_checkpoint = options[load_type]['remove_checkpoint']
create_database = options[load_type]['create_database']
create_table = options[load_type]['create_table']
trigger = options[load_type]['trigger']


spark.conf.set("c.database_name",database_name)
spark.conf.set("c.table_name",table_name)
spark.conf.set("c.database_table_name",database_table_name)


print(F"""
input_path      {input_path}, 
output_path     {output_path}, 
checkpoint_path {checkpoint_path}, 
database table  {database_name}.{table_name} => {database_table_name},

load_type       {load_type}
==>
  remove checkpoint:       {remove_checkpoint}
  includeExistingFiles:    {includeExistingFiles}
  maxFilesPerTrigger:      {maxFilesPerTrigger}
  mergeSchema:             {mergeSchema}
  mode:                    {mode}
  trigger:                 {trigger}
  create_database          {create_database}
  create_table             {create_table}
""")

# COMMAND ----------

# DBTITLE 1,Optionally remove checkpoint
if remove_checkpoint:
  print(F"Removing checkpoint {checkpoint_path}")
  dbutils.fs.rm(dir=checkpoint_path, recurse=True)

# COMMAND ----------

if create_table:
  print("Removing target table data {output_path}")
  dbutils.fs.rm(dir=output_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Define cloud trail schema
from pyspark.sql.types import StructType, StringType, StructField, StringType, BooleanType, MapType, ArrayType
input_format = "json"
schema = StructType([
    StructField("Records", ArrayType(
        StructType([
            StructField("additionalEventData", StringType(), True),
            StructField("apiVersion", StringType(), True),
            StructField("awsRegion", StringType(), True),
            StructField("errorCode", StringType(), True),
            StructField("errorMessage", StringType(), True),
            StructField("eventID", StringType(), True),
            StructField("eventName", StringType(), True),
            StructField("eventSource", StringType(), True),
            StructField("eventTime", StringType(), True),
            StructField("eventType", StringType(), True),
            StructField("eventVersion", StringType(), True),
            StructField("readOnly", BooleanType(), True),
            StructField("recipientAccountId", StringType(), True),
            StructField("requestID", StringType(), True),
            StructField("requestParameters", MapType(
                StringType(), StringType(), True), True),
            StructField("resources", ArrayType(
                StructType([
                    StructField("ARN", StringType(), True),
                    StructField("accountId", StringType(), True),
                    StructField("type", StringType(), True)
                ]),
                True), True),
            StructField("responseElements", MapType(
                StringType(), StringType(), True), True),
            StructField("sharedEventID", StringType(), True),
            StructField("sourceIPAddress", StringType(), True),
            StructField("serviceEventDetails", MapType(
                StringType(), StringType(), True), True),
            StructField("userAgent", StringType(), True),
            StructField("userIdentity",
                        StructType([
                            StructField("accessKeyId", StringType(), True),
                            StructField("accountId", StringType(), True),
                            StructField("arn", StringType(), True),
                            StructField("invokedBy", StringType(), True),
                            StructField("principalId", StringType(), True),
                            StructField("sessionContext",
                                        StructType([
                                            StructField("attributes",
                                                        StructType([
                                                            StructField(
                                                                "creationDate", StringType(), True),
                                                            StructField(
                                                                "mfaAuthenticated", StringType(), True)
                                                        ]), True),
                                            StructField("sessionIssuer",
                                                        StructType([
                                                            StructField(
                                                                "accountId", StringType(), True),
                                                            StructField(
                                                                "arn", StringType(), True),
                                                            StructField(
                                                                "principalId", StringType(), True),
                                                            StructField(
                                                                "type", StringType(), True),
                                                            StructField(
                                                                "userName", StringType(), True)
                                                        ]), True)
                                        ]), True),
                            StructField("type", StringType(), True),
                            StructField("userName", StringType(), True),
                            StructField("webIdFederationData",
                                        StructType([
                                            StructField("federatedProvider",
                                                        StringType(), True),
                                            StructField("attributes", MapType(
                                                StringType(), StringType(), True), True)
                                        ]), True)
                        ]), True),

            StructField("vpcEndpointId", StringType(), True)
        ]), True), True)
      ])


# COMMAND ----------

# DBTITLE 1,Run Historical,Incremental or Continious ingest of CloudTrail into Delta table

from pyspark.sql.functions import explode, unix_timestamp, input_file_name, col

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)
spark.conf.set("spark.databricks.cloudFiles.asyncDirListing", True)

# runs asynchronously
pre_query = (
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", input_format)
    .option("cloudFiles.schemaLocation", output_path)
    .option("cloudFiles.includeExistingFiles", includeExistingFiles)
    .schema(schema)
    .load(input_path)
  # transformations
    .select(explode("Records").alias("record"))
  # add lineage / audit columns
    .select(unix_timestamp("record.eventTime", "yyyy-MM-dd'T'hh:mm:ss").cast("timestamp").alias("timestamp"),
            "record.*")
    .withColumn("eventDate", col("eventTime").cast("date").alias())
    .withColumn("input_file_name", input_file_name())
  .writeStream
    .format("delta")
    .outputMode(mode)
    .option("mergeSchema", mergeSchema)
    .option("checkpointLocation", checkpoint_path)
    .option("maxFilesPerTrigger", maxFilesPerTrigger)
    .option("path", output_path)
)

# manage trigger types between historical, incremental and continious
if load_type in ['historical', 'incremental']:
  query = (pre_query
    .trigger(once=True)
    .start())
elif load_type in ['continuous']:
  query = (pre_query
    .trigger(processingTime=trigger['value'])
    .start())
else:
  print(F"Invalid load_type {load_type}")

# COMMAND ----------

# DBTITLE 1,Monitor Stream startup
import time

while query.isActive:
  t = time.localtime()
  current_time = time.strftime("%H:%M:%S", t)
  print(current_time, query.status, query.recentProgress, query.lastProgress)
  time.sleep(10)

# COMMAND ----------

# DBTITLE 1,Wait until ingest job is complete
query.awaitTermination()

# COMMAND ----------

display(dbutils.fs.ls(output_path))

# COMMAND ----------

if create_database:
  spark.sql(F"""CREATE DATABASE IF NOT EXISTS {database_name}""" )

if create_table:
  spark.sql(F"DROP TABLE IF EXISTS {database_table_name}")
  spark.sql(F"""CREATE TABLE {database_table_name} USING DELTA LOCATION '{output_path}'""")

# COMMAND ----------

# MAGIC %sql describe history ${c.database_table_name}

# COMMAND ----------

# MAGIC %sql 
# MAGIC OPTIMIZE ${c.database_table_name}

# COMMAND ----------

# MAGIC %sql describe history ${c.database_table_name}

# COMMAND ----------

# MAGIC %md ## Validate CloudTrail Load

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${c.database_name}.${c.table_name} limit 10

# COMMAND ----------

# DBTITLE 1,Events per file
# MAGIC %sql
# MAGIC select 
# MAGIC   count(1) rows, 
# MAGIC   count(distinct input_file_name) files, 
# MAGIC   count(1)/count(distinct input_file_name) ratio
# MAGIC from ${c.database_table_name}

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT awsRegion, count(1) count, count(distinct input_file_name) files
# MAGIC FROM ${c.database_table_name}
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT awsRegion, count(1) count, count(distinct input_file_name) files
# MAGIC FROM ${c.database_table_name}
# MAGIC group by 1
# MAGIC order by 1

# COMMAND ----------

# MAGIC %md ### Use BinaryFile to count the files
# MAGIC - To double check our loads, uses binaryFile type to gather file meta data

# COMMAND ----------

spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", True)
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.autoCompact", True)
df = (
  spark.read
  .format("binaryFile")
  .option("pathGlobFilter",      "*.json.gz")
  .option("recursiveFileLookup", "true")
  .load(input_path)
  .drop('content')
)
from pyspark.sql.functions import split
df.withColumn('tags',split(df.path,'/')).write.format("delta").mode('overwrite').saveAsTable(F"{database_table_name}_count")

spark.conf.set("c.file_count_tbl",F"{database_table_name}_count")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) 
# MAGIC FROM ${c.file_count_tbl}

# COMMAND ----------

# MAGIC %sql 
# MAGIC select tags[5], tags[6], count(1) files
# MAGIC from ${c.file_count_tbl}
# MAGIC group by tags[5], tags[6]
# MAGIC --where tags[5] != 'CloudTrail'
# MAGIC --limit 10

# COMMAND ----------

# MAGIC %md ## The End

# COMMAND ----------


