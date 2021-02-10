-- Databricks notebook source
set c.my_special_constant = 42;
set c.my_path = "/dbfs/mnt/my_bucket/my_folder";

select ${c.my_special_constant}, ${c.my_path};

-- COMMAND ----------

-- MAGIC %python
-- MAGIC my_special_constant = spark.conf.get("c.my_special_constant")
-- MAGIC spark.conf.set("c.my_other_var","53")
-- MAGIC my_other_var = spark.sql("select ${c.my_other_var}").collect()[0][0]
-- MAGIC print(my_special_constant, 
-- MAGIC       my_other_var, 
-- MAGIC       spark.conf.get("c.my_special_constant"), 
-- MAGIC       spark.conf.get("c.my_other_var"))

-- COMMAND ----------

select ${c.my_other_var}

-- COMMAND ----------

