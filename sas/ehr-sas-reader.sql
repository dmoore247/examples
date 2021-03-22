-- Databricks notebook source
-- MAGIC %sql
-- MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
-- MAGIC set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/shared_uploads/brad.heide@databricks.com/rwe/

-- COMMAND ----------

create database IF NOT EXISTS sas_rwe

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import input_file_name
-- MAGIC path = "dbfs:/FileStore/shared_uploads/brad.heide@databricks.com/rwe/"
-- MAGIC target_db = "sas_rwe"
-- MAGIC 
-- MAGIC cols = "*"
-- MAGIC files = dbutils.fs.ls(path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #setup function to load one file using spark
-- MAGIC def sas_read_write_delta(file_name):
-- MAGIC   table_name = file_name.split('/')[-1].replace('.sas7bdat','')
-- MAGIC   print(F"{file_name} >\t  {table_name}\n")
-- MAGIC   (
-- MAGIC     spark
-- MAGIC       .read
-- MAGIC       .format("com.github.saurfang.sas.spark")
-- MAGIC       .load(file_name, 
-- MAGIC             forceLowercaseNames=True, 
-- MAGIC             inferLong=True, 
-- MAGIC             metadataTimeout=60)
-- MAGIC       .withColumn("FILE_PATH", input_file_name())
-- MAGIC       .select(cols)
-- MAGIC       .write
-- MAGIC         .format("delta")
-- MAGIC         .mode("overwrite")
-- MAGIC         .saveAsTable(F"{target_db}.{table_name}")
-- MAGIC   )
-- MAGIC 
-- MAGIC # run (4) loads in parallel, each load runs in parallel by splitting the source file list
-- MAGIC # Delta Lake tables support concurrent writes
-- MAGIC if __name__ == '__main__':
-- MAGIC     from multiprocessing.pool import ThreadPool
-- MAGIC     pool = ThreadPool(4)
-- MAGIC     pool.map(
-- MAGIC       lambda file_name: sas_read_write_delta(file_name.path), files)

-- COMMAND ----------

show tables in sas_rwe

-- COMMAND ----------

describe history sas_rwe.careplans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def summary_csv(file_path:str):
-- MAGIC   print(F"Processing... {file_path}")
-- MAGIC   df = spark.read.option('header','true').csv(F'{file_path}')
-- MAGIC   sum = df.selectExpr('count(*) as count').withColumn('table',lit(file_path))
-- MAGIC   #display(sum)
-- MAGIC   sum.write.mode('append').save('dbfs:/tmp/rwe_abc')
-- MAGIC   return sum

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC def summary(table_name:str):
-- MAGIC   print(F"Processing... {table_name}")
-- MAGIC   df = spark.read.table(F'{table_name}')
-- MAGIC   sum = df.selectExpr('count(*) as count').withColumn('table',lit(table_name))
-- MAGIC   #display(sum)
-- MAGIC   sum.write.mode('append').save('dbfs:/tmp/rwe_abc')
-- MAGIC   return sum
-- MAGIC summary('sas_rwe.careplans')

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/tmp/rwe_abc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC lst = spark.catalog.listTables(target_db)
-- MAGIC tables = [F"{target_db}.{tbl.name}" for tbl in lst]
-- MAGIC print(tables)
-- MAGIC for table in tables:
-- MAGIC   summary(table)

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/tmp/rwe_abc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC csv_path = "/dbfs/databricks-datasets/rwe/ehr/csv/"
-- MAGIC files = spark.read.option("pathGlobFilter", "*.csv").format("binaryFile").load(csv_path.replace('/dbfs','dbfs:')).drop('content').select('path')
-- MAGIC files = [f[0] for f in files.collect()]
-- MAGIC print(files)
-- MAGIC for f in files:
-- MAGIC   summary_csv(f)

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/rwe/ehr/csv/

-- COMMAND ----------

show tables in sas_rwe

-- COMMAND ----------

-- MAGIC %sql select * from delta.`dbfs:/tmp/rwe_abc` order by count, table

-- COMMAND ----------


