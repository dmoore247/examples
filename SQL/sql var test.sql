-- Databricks notebook source
set c.my_special_constant = 42;
set c.my_path = "/dbfs/mnt/my_bucket/my_folder";

select ${c.my_special_constant}, ${c.my_path};

-- COMMAND ----------


