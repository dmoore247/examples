-- Databricks notebook source
show CATALOGS like '*uc_demo_upgraded_douglas_moore*'

-- COMMAND ----------

use catalog uc_demo_upgraded_douglas_moore;
use schema douglas_external;

-- COMMAND ----------

show tables

-- COMMAND ----------

describe extended external_table_1

-- COMMAND ----------

-- MAGIC %fs ls s3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/bronze.db/external_table_1/_delta_log

-- COMMAND ----------

-- MAGIC %fs head s3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/bronze.db/external_table_1/_delta_log/00000000000000000000.json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.format('delta').load("s3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/bronze.db/external_table_1")
-- MAGIC display(df)

-- COMMAND ----------

describe external_table_1

-- COMMAND ----------

insert into external_table_1(a)
select id as a from range(100)

-- COMMAND ----------

insert into external_table_1(a) values (1)

-- COMMAND ----------

insert into external_table_1(a) values (1,NULL, null, null, NULL)

-- COMMAND ----------

insert into external_table_1(a,b,c,d,e) values (1,NULL, null, null, NULL)

-- COMMAND ----------



-- COMMAND ----------

LIST 's3://one-env-uc-external-location/shared_location'

-- COMMAND ----------

-- MAGIC %fs mkdirs s3://one-env-uc-external-location/shared_location/douglas_moore

-- COMMAND ----------

-- MAGIC %fs mkdirs s3://one-env-uc-external-location/shared_location/douglas_moore/external.db

-- COMMAND ----------


