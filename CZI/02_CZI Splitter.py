# Databricks notebook source
# DBTITLE 1,Import utilities and setup (resets context)
# MAGIC %run "./czi-utils.py"

# COMMAND ----------

# MAGIC %md ## Get input files

# COMMAND ----------

path = "/dbfs/FileStore/shared_uploads/douglas.moore@databricks.com/2017_08_03__0006.czi"
df = dbutils.fs.ls(path.replace('/dbfs','dbfs:'))

# COMMAND ----------

pattern = '*.czi'
parallelism = 64
objects_df = (spark.read.format("binaryFile")
  .option("pathGlobFilter", pattern)
  .option("recursiveFileLookup", "true")
  .load(path.replace('/dbfs','dbfs:'))
  .drop('content')
  .repartition(parallelism)
)

# COMMAND ----------

display(objects_df)

# COMMAND ----------

paths_pdf = objects_df.cache().toPandas()

# COMMAND ----------

# unit test czi_meta
pdfs = czi_meta(iter([paths_pdf]))
for pdf in pdfs:
  display(pdf)

# COMMAND ----------

# MAGIC %md ## Extract Metadata

# COMMAND ----------

meta_df = objects_df.mapInPandas(czi_meta, schema=czi_meta_schema)
display(meta_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Setup Parallel generation

# COMMAND ----------

# DBTITLE 1,Setup for parallel tile / patch generation over all of the CZI images
from pyspark.sql.functions import sequence, explode, col, lit, array
dim = 'M'
tasks_df = (
  meta_df
  .filter(F"dim = '{dim}'")
  .withColumn('C_index', lit(0))
  .withColumn('starts',array(lit(0),lit(508)))
  .withColumn('M_index',explode(
    sequence(
      col('dim_index_min'),
      col('dim_index_max'))))
)
display(tasks_df)

# COMMAND ----------

tasks_df.cache().count()

# COMMAND ----------

# MAGIC %md ## Run Parallel Operations

# COMMAND ----------

# DBTITLE 1,Run Tiling Operations
tiles_df = tasks_df.repartition(64).mapInPandas(czi_tiler, czi_tiler_schema)
#tiles_df.limit(10).count()
display(tiles_df.limit(10))

# COMMAND ----------

tasks_df.count(), tasks_df.selectExpr("max(M_index)").collect()[0][0]

# COMMAND ----------

tiles_df.count(), tiles_df.selectExpr("max(M_index)").collect()[0][0]

# COMMAND ----------

delta_path = 'dbfs:/tmp/czi_tiles_dela'
dbutils.fs.rm(delta_path,True)
tiles_df.write.format('delta').mode('overwrite').save(delta_path)

# COMMAND ----------

