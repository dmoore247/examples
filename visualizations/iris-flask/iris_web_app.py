# Databricks notebook source
def get_dbutils(spark):
    try:
        print(">> get_dbutils.1")
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        print(">> get_dbutils.2")
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils


# Run spark SQL from IDE
# Local code, remote Spark workers & driver, data on cloud storage
# Requires python package:
#   databricks-connect matching your cluster runtime version
#
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import LongType

# create spark session
print("Creating Spark Session")
spark = (SparkSession
    .builder
    .appName("SQL Query")
    .config('spark.ui.enabled','false')
    .getOrCreate())

##
dbutils = get_dbutils(spark)

username = dbutils.secrets.get("oetrta", "redshift-username")
password = dbutils.secrets.get("oetrta", "redshift-password")
redshift_endpoint = dbutils.secrets.get(scope = "oetrta", key = "redshift-cluster-endpoint")
tempdir = dbutils.secrets.get(scope = "oetrta", key = "redshift-temp-dir")
iam_role = dbutils.secrets.get(scope = "oetrta", key = "redshift-iam-role")


##
table = "douglas_iris"

##
jdbcUrl = "jdbc:redshift://{}/dev?user={}&password={}".format(redshift_endpoint, username, password)
#print(jdbcUrl)


##
sql_string = """select * from douglas_iris"""


##
read_df = spark.read \
  .format("com.databricks.spark.redshift") \
  .option("url", jdbcUrl) \
  .option("query", sql_string) \
  .option("tempdir", tempdir) \
  .option("aws_iam_role", iam_role)\
  .load()
data = read_df.toPandas().to_json(orient='records')

with open('iris_template.html','r') as f:
  html_template = f.read()

##
print(html_template % data)
