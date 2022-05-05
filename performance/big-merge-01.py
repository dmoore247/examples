# Databricks notebook source
# MAGIC %md # Self Merge Performance Test
# MAGIC 
# MAGIC - Create large & wide delta table
# MAGIC - Run UPSERT / MERGE INTO simulating an apply operation from Attunity

# COMMAND ----------

# MAGIC %md ## Generate data set then run self merge

# COMMAND ----------

from sklearn.datasets import make_classification
import pandas as pd
import numpy as np

def make_random_dataframe(n_samples = 10_000, n_cols=10):
  X1, Y1 = make_classification(
    n_samples = n_samples,
    n_features= n_cols  - 1
  )
  X1.shape
  pdf = pd.DataFrame(X1)
  pdf['id'] = range(X1.shape[0])

  i=1
  pdf[i] = (1000. * pdf[i]).astype(np.int64)
  i += 1
  pdf[i] = 'c'+pdf[i].astype(str)

  i += 1
  pdf[i] = pdf[i].astype('datetime64[ns]')

  cols = pdf.columns.values
  pdfx=pdf[list(cols[-1:])+list(cols[:-1])]
  
  pdfx.rename(columns={1:"bigint_1", 2:"str_2", 3:"dt_3"}, inplace=True)
  df = spark.createDataFrame(pdfx)
  return df

# COMMAND ----------

table_name = 'dm_merge'
database_name = 'default'
email = dbutils.entry_point.getDbutils().notebook().getContext().userName().get()
path = F"dbfs:/tmp/{email}/{database_name}.db/{table_name}"
spark.conf.set('c.path', path)

if spark._jsparkSession.catalog().tableExists(database_name, table_name):
  print(F"{table_name} already exists")
else:
  df = make_random_dataframe(n_samples = 9_000_000, n_cols = 180)

  dbutils.fs.rm(path, recurse=True)
  df.write.format('delta').mode('overwrite').option('mergeSchema','true').save(path)
  spark.sql("DROP TABLE IF EXISTS dm_merge;")
  spark.sql(F"CREATE EXTERNAL TABLE dm_merge USING DELTA LOCATION '{path}'")

# COMMAND ----------

from pyspark.sql import functions as F
def analyze_history(table_name):
  dfh = spark.sql(F"""DESCRIBE HISTORY {table_name}""")
  keys_df = dfh.select(F.explode(F.map_keys(F.col("operationMetrics")))).distinct()
  keys = list(map(lambda row: row[0], keys_df.collect()))
  key_cols = list(map(lambda f: F.col("operationMetrics").getItem(f).alias(str(f)), keys))
  final_cols = [F.col("version"), F.col("timestamp"), F.col("operation")] + key_cols
  return dfh.select(final_cols)


final_df = analyze_history("default.dm_merge")
display(final_df)

# COMMAND ----------

display(dfh)

# COMMAND ----------

# MAGIC %sql delete from default.dm_merge where id = int(id / 30)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ### Validate data set

# COMMAND ----------

dfx = spark.read.table(F"{database_name}.{table_name}")

# COMMAND ----------

# MAGIC %md ### Merge
# MAGIC ```sql
# MAGIC MERGE INTO target_table_identifier [AS target_alias]
# MAGIC USING source_table_identifier [<time_travel_version>] [AS source_alias]
# MAGIC ON <merge_condition>
# MAGIC [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
# MAGIC [ WHEN MATCHED [ AND <condition> ] THEN <matched_action> ]
# MAGIC [ WHEN NOT MATCHED [ AND <condition> ]  THEN <not_matched_action> ]
# MAGIC ```
# MAGIC ```sql
# MAGIC <merge_condition> =
# MAGIC   How the rows from one relation are combined with the rows of another relation. An expression with a return type of Boolean.
# MAGIC 
# MAGIC <matched_action>  =
# MAGIC   DELETE  |
# MAGIC   UPDATE SET *  |
# MAGIC   UPDATE SET column1 = value1 [, column2 = value2 ...]
# MAGIC 
# MAGIC <not_matched_action>  =
# MAGIC   INSERT *  |
# MAGIC   INSERT (column1 [, column2 ...]) VALUES (value1 [, value2 ...])
# MAGIC 
# MAGIC <time_travel_version>  =
# MAGIC   TIMESTAMP AS OF timestamp_expression |
# MAGIC   VERSION AS OF version
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dm_merge as target
# MAGIC USING dm_merge as source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN update set *
# MAGIC WHEN NOT MATCHED THEN insert *

# COMMAND ----------

# MAGIC %sql set spark.databricks.clusterUsageTags.sparkVersion

# COMMAND ----------

# MAGIC %sql set

# COMMAND ----------

spark.sparkContext.setJobGroup("dm_merge", "enable low shuffle merge")
spark.sql("""
--- api call
MERGE INTO dm_merge as target
USING dm_merge VERSION AS OF 0 as source 
ON target.id = source.id
WHEN MATCHED THEN update set *
WHEN NOT MATCHED THEN insert *""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.merge.enableLowShuffle = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- low shuffle merge x2
# MAGIC MERGE INTO dm_merge as target
# MAGIC USING dm_merge VERSION AS OF 0 as source 
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN update set *
# MAGIC WHEN NOT MATCHED THEN insert *

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO dm_merge as target
# MAGIC USING dm_merge VERSION AS OF 0 as source 
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN update set *
# MAGIC WHEN NOT MATCHED THEN insert *

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history dm_merge
