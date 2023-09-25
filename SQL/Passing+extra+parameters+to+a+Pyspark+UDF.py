# Databricks notebook source
# DBTITLE 1,Passing extra parameters to a Pyspark UDF
#
# Demo passing parameters to a Pyspark UDF
#

from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
          StructField("v1",StringType()),
          StructField("v2",LongType()),
          StructField("v3",StringType())
])

#
# This outer function will pass parameters as context to an instance of the UDF.
# These parameters will remain constant for the lifetime of the UDF function.
#
def get_udf(p1:str, p2:int):
  
  # this function will become the UDF
  def api_call(id:str):
    print(id, p1, p2)
    return {'v1':p1,'v2':p2,'v3':id}
  
  return udf(api_call, schema)

#
# test the UDF
#
df = spark.range(100, numPartitions=10)
the_udf = get_udf("p1", 678)

from pyspark.sql.functions import col
df2 = df.withColumn("results",the_udf(col('id')))
display(df2)

# COMMAND ----------


