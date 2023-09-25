# Databricks notebook source
# DBTITLE 1,Passing extra parameters to a Pyspark UDF
#
# Demo passing parameters to a Pyspark UDF
#

from pyspark.sql.functions import udf, explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests

schema = StructType([
          StructField("json",StringType()),
          StructField("status_code",IntegerType()),
          StructField("error",StringType())
])

#
# This outer function will pass parameters as context to an instance of the UDF.
# These parameters will remain constant for the lifetime of the UDF function.
#
def get_udf(endpoint_url:str, accept:str):
  
  # this function will become the UDF
  def api_call(id:str):
    error = None
    content = None
    json = None
    request_url = None
    raw = None
    status_code = None
   
    try:
      print(endpoint_url, accept, id)
      headers = {'Accept': accept, 'Content-Type': accept}
      payload = {'id': id}
      r = requests.get(url=endpoint_url, params = payload, headers = headers)
      print(r)
      json = r.json()
      status_code = r.status_code
      # z = 1.0/0.0 # simulate exception
    except Exception as e:
      print(e)
      error = str(e)

    return {
      'json':json,
      'status_code':status_code,
      'error':error
    }
  
      
  return udf(api_call, schema)
  

#
# test the UDF
#
df = spark.range(10, numPartitions=5)
the_udf = get_udf("https://httpbin.org/uuid", accept="application/json")

from pyspark.sql.functions import col
df2 = df.withColumn("results",(the_udf(col('id'))))
display(df2)
