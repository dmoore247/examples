-- Databricks notebook source
--drop table douglas_moore.demo.schema_history;

SET var.schema_history_table = {{schema_history_table}}

CREATE TABLE IF NOT EXISTS ${var.schema_history_table} USING delta
AS SELECT *, cast(now() as TIMESTAMP) as _change_datetime 
FROM main.information_schema.columns 
WHERE 1 = 0;


-- Perform a MERGE statement to update the `douglas_moore.demo.schema_drift` table
MERGE INTO ${var.schema_history_table} dest
USING (
  SELECT table_catalog, table_schema, table_name, column_name, 
          ordinal_position, column_default, is_nullable, data_type, 
          character_maximum_length, numeric_precision, numeric_scale, cast(now() as TIMESTAMP) as _change_datetime 
  FROM main.information_schema.columns 
  WHERE table_catalog = {{catalog}}) src
ON dest.table_catalog = src.table_catalog
  AND dest.table_schema = src.table_schema
  AND dest.table_name = src.table_name
  AND dest.column_name = src.column_name
WHEN NOT MATCHED THEN
  INSERT (table_catalog, table_schema, table_name, column_name, 
          ordinal_position, column_default, is_nullable, data_type, 
          character_maximum_length, numeric_precision, numeric_scale, _change_datetime)
  VALUES (src.table_catalog, src.table_schema, src.table_name, src.column_name, 
          src.ordinal_position, src.column_default, src.is_nullable, src.data_type, 
          src.character_maximum_length, src.numeric_precision, src.numeric_scale, src._change_datetime);



CREATE TABLE main.douglas_moore.test_schema_drift (a int, b string, c date);

select * from douglas_moore.demo.schema_history order by _change_datetime DESC limit 10;

-- COMMAND ----------


