# Databricks notebook source
# MAGIC %md # Scaling Analytics
# MAGIC - Review the data
# MAGIC - SQL
# MAGIC - Pandas
# MAGIC - Koalas
# MAGIC - PySpark
# MAGIC - SparkR
# MAGIC - SparklyR

# COMMAND ----------

# MAGIC %md ## Review the data

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/rwe/ehr/csv

# COMMAND ----------

# MAGIC %sh wc /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv

# COMMAND ----------

# DBTITLE 1,Make a bigger dataset
# MAGIC %sh 
# MAGIC mkdir -p /dbfs/tmp/observations
# MAGIC cp /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv /dbfs/tmp/observations/1.csv
# MAGIC cp /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv /dbfs/tmp/observations/2.csv
# MAGIC cp /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv /dbfs/tmp/observations/3.csv
# MAGIC cp /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv /dbfs/tmp/observations/4.csv
# MAGIC cp /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv /dbfs/tmp/observations/5.csv
# MAGIC cp /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv /dbfs/tmp/observations/6.csv
# MAGIC cp /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv /dbfs/tmp/observations/7.csv
# MAGIC cp /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv /dbfs/tmp/observations/8.csv

# COMMAND ----------

# MAGIC %sh head -5 /dbfs/databricks-datasets/rwe/ehr/csv/observations.csv

# COMMAND ----------

# MAGIC %md ## SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS OBSERVATIONS;
# MAGIC CREATE TEMPORARY VIEW OBSERVATIONS
# MAGIC USING CSV
# MAGIC OPTIONS (header = true, path="dbfs:/tmp/observations");
# MAGIC 
# MAGIC SELECT * from OBSERVATIONS;

# COMMAND ----------

# MAGIC %sql select count(*) count from OBSERVATIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT code, count(*) count 
# MAGIC FROM OBSERVATIONS 
# MAGIC GROUP BY code 
# MAGIC ORDER BY count(*) DESC

# COMMAND ----------

# MAGIC %md ## Pandas

# COMMAND ----------

# DBTITLE 1,Single File
import pandas as pd
path = "/dbfs/databricks-datasets/rwe/ehr/csv/observations.csv"

pd.read_csv(path).count()

# COMMAND ----------

# DBTITLE 1,Full data set - count
import glob, os
path = "/dbfs/tmp/observations"                     
all_files = glob.glob(os.path.join(path, "*.csv"))

pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True).count()

# COMMAND ----------

# DBTITLE 1,Full data set - group by count order by desc
( pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
   .groupby('CODE')['CODE']
   .count()
   .sort_values(ascending=False)
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Koalas

# COMMAND ----------

import databricks.koalas as ks
import pandas as pd

ks.options.display.max_rows = 10

ks.read_csv(path.replace('/dbfs','dbfs:')).count()

# COMMAND ----------

# DBTITLE 1,Full data set - group by count order by desc
ks.read_csv(path.replace('/dbfs','dbfs:')).groupby('CODE')['CODE'].count().sort_values(ascending=False)

# COMMAND ----------

# MAGIC %md ## PySpark

# COMMAND ----------

from pyspark.sql.functions import col
display( 
  spark.read
    .option('header','true')
    .csv(path.replace('/dbfs','dbfs:'))
    .groupBy('CODE')
    .count()
    .orderBy(col('count').desc())
)

# COMMAND ----------

# MAGIC %md ## SparkR

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)

# COMMAND ----------

# MAGIC %r
# MAGIC df <- read.df("dbfs:/databricks-datasets/rwe/ehr/csv/observations.csv",
# MAGIC        source = "csv", header = "true", inferSchema = "true", na.strings = "NA")
# MAGIC 
# MAGIC code_counts <- summarize(groupBy(df, df$code), count = n(df$code))
# MAGIC head(arrange(code_counts, desc(code_counts$count)), num=10)

# COMMAND ----------

# MAGIC %r
# MAGIC df <- read.df("dbfs:/tmp/observations",
# MAGIC        source = "csv", header = "true", inferSchema = "true", na.strings = "NA")
# MAGIC 
# MAGIC code_counts <- summarize(groupBy(df, df$code), count = n(df$code))
# MAGIC head(arrange(code_counts, desc(code_counts$count)), num=10)

# COMMAND ----------

# MAGIC %md ## SparklyR

# COMMAND ----------

# MAGIC %r
# MAGIC # Installing latest version of Rcpp
# MAGIC install.packages("Rcpp") 
# MAGIC 
# MAGIC if (!require("sparklyr")) {
# MAGIC   install.packages("sparklyr")  
# MAGIC }

# COMMAND ----------

# MAGIC %r
# MAGIC library(sparklyr)
# MAGIC library(dplyr)

# COMMAND ----------

# MAGIC %r
# MAGIC sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %r
# MAGIC df <- spark_read_csv(
# MAGIC   sc,
# MAGIC   name = "observations",
# MAGIC   path = "dbfs:/databricks-datasets/rwe/ehr/csv/observations.csv",
# MAGIC   header = TRUE
# MAGIC )
# MAGIC group_by(df, CODE) %>% 
# MAGIC   count() %>%
# MAGIC   arrange(desc(n))

# COMMAND ----------

# MAGIC %r
# MAGIC df <- spark_read_csv(
# MAGIC   sc,
# MAGIC   name = "observations",
# MAGIC   path = "dbfs:/tmp/observations",
# MAGIC   header = TRUE
# MAGIC )
# MAGIC group_by(df, CODE) %>% 
# MAGIC   count() %>%
# MAGIC   arrange(desc(n))

# COMMAND ----------

# MAGIC %md 
# MAGIC # Thank you very much
# MAGIC # どうもありがとうございました

# COMMAND ----------


