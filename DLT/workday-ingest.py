# Databricks notebook source
# MAGIC %md # Pull Workday defined reports into the Lakehouse bronze layer
# MAGIC The methods below use Python's requests library to call the workday endpoint api, retrieve the JSON formatted results, parse and insert into Delta Lakeh.

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
from pyspark.sql import functions as F

def workday_source(connect_string = 'connection_workday'):
  """
    Workday connector for DLT:
    With connection parameters, connect to workday and download report contents and return as a Spark DataFrame.
  """
  url, user, password =  dbutils.secrets.get(connect_string,'url'), dbutils.secrets.get(connect_string,'user'), dbutils.secrets.get(connect_string,'password')
  req_url = url+'?format=json'
  response = requests.get(req_url, auth=HTTPBasicAuth(user, password))
  df = spark.read.json(spark.sparkContext.parallelize([str(response.json())]))
  return df.select(F.explode('Report_Entry')).select('col.*')

#workday_source().display()

# COMMAND ----------

import dlt  # runs only under DLT engine

@dlt.table(
  name="workday_employee_source",
  comment="Source employee list from Workday",
  )

def workday_employee_source():
    return workday_source('connection_workday')

# COMMAND ----------


