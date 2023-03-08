# Databricks notebook source
# MAGIC %sql
# MAGIC create database uc_demo_upgraded_douglas_moore.douglas_moore_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uc_demo_upgraded_douglas_moore.douglas_moore_gold.big11
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/big11'
# MAGIC AS SELECT * 
# MAGIC FROM RANGE(1000)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into uc_demo_upgraded_douglas_moore.douglas_moore_gold.big11
# MAGIC SELECT * from range(1001,100000000)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from uc_demo_upgraded_douglas_moore.douglas_moore_gold.big11

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uc_demo_upgraded_douglas_moore.douglas_moore_gold.big11
# MAGIC SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5')

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.schema.autoMerge.enabled=true;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS uc_demo_upgraded_douglas_moore.douglas_moore_gold.big15
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks-e2demofieldengwest/douglas_moore_sandbox_demos/big15'
# MAGIC AS
# MAGIC SELECT id, 
# MAGIC   cast(id as string) as id_str, 
# MAGIC   randn() randn_num, 
# MAGIC   cast(randn(0) as string) randn_str, 
# MAGIC   cast(floor(randn()*100.0) as int) randn_floor,
# MAGIC   reverse(cast(id as string)) as id_reverse,
# MAGIC   cast(now() as timestamp) as update_ts
# MAGIC FROM RANGE(500000000)

# COMMAND ----------

set spark.databricks.delta.schema.autoMerge.enabled=true;

INSERT OVERWRITE uc_demo_upgraded_douglas_moore.douglas_moore_gold.big12
SELECT id, cast(id as string) as id_str, reverse(cast(id as string)) as id_reverse, cast(now() as timestamp) as update_ts
FROM RANGE(100000000)
