-- Databricks notebook source
-- MAGIC %sql
-- MAGIC set c.my_special_constant = 42;
-- MAGIC set c.my_path = "/dbfs/mnt/my_bucket/my_folder";
-- MAGIC 
-- MAGIC select ${c.my_special_constant}, ${c.my_path};

-- COMMAND ----------


