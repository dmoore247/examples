-- Databricks notebook source
-- MAGIC %md # COPY INTO demo

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS douglas_moore_bronze LOCATION 's3a://oetrta/dmoore/databases/bronze';
DESCRIBE DATABASE douglas_moore_bronze;

-- COMMAND ----------

SHOW TABLES in douglas_moore_bronze;

-- COMMAND ----------

-- MAGIC %md ## Load Data

-- COMMAND ----------

--
-- COPY INTO is idempotent by default
-- load bronze delta tables
-- COPY command   TARGET path                                        SOURCE path                                                    FORMAT
--
COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/iotv1`      FROM 'dbfs:/databricks-datasets/iot'                    FILEFORMAT = JSON;
COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/userv1`     FROM 'dbfs:/databricks-datasets/iot-stream/data-user'   FILEFORMAT = CSV  FORMAT_OPTIONS('header'='true', 'inferSchema' = 'true');
COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/devicev1`   FROM 'dbfs:/databricks-datasets/iot-stream/data-device' FILEFORMAT = JSON;
--

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS douglas_moore_bronze.iot    USING DELTA LOCATION 's3a://oetrta/dmoore/databases/bronze/iotv1';
CREATE TABLE IF NOT EXISTS douglas_moore_bronze.user   USING DELTA LOCATION 's3a://oetrta/dmoore/databases/bronze/userv1';
CREATE TABLE IF NOT EXISTS douglas_moore_bronze.device USING DELTA LOCATION 's3a://oetrta/dmoore/databases/bronze/devicev1';

SHOW TABLES IN douglas_moore_bronze;

-- COMMAND ----------

SHOW TABLES in douglas_moore_bronze;

-- COMMAND ----------

-- MAGIC %md ### Verify

-- COMMAND ----------

-- Do the count
SELECT 'iot'    as name, COUNT(*) count FROM douglas_moore_bronze.iot
UNION ALL
SELECT 'user'   as name, COUNT(*) count FROM douglas_moore_bronze.user
UNION ALL
SELECT 'device' as name, COUNT(*) count FROM douglas_moore_bronze.device;

-- COMMAND ----------

-- MAGIC %md ## Load again

-- COMMAND ----------

COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/iotv1`      FROM 'dbfs:/databricks-datasets/iot'                    FILEFORMAT = JSON;
COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/userv1`     FROM 'dbfs:/databricks-datasets/iot-stream/data-user'   FILEFORMAT = CSV  FORMAT_OPTIONS('header'='true', 'inferSchema' = 'true');
COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/devicev1`   FROM 'dbfs:/databricks-datasets/iot-stream/data-device' FILEFORMAT = JSON;
--

-- COMMAND ----------

-- MAGIC %md ### Verify
-- MAGIC - no duplicates

-- COMMAND ----------

-- Do the count
SELECT 'iot'    as name, COUNT(*) count FROM douglas_moore_bronze.iot
UNION ALL
SELECT 'user'   as name, COUNT(*) count FROM douglas_moore_bronze.user
UNION ALL
SELECT 'device' as name, COUNT(*) count FROM douglas_moore_bronze.device;

-- COMMAND ----------

-- MAGIC %md ## Force load

-- COMMAND ----------

--
-- load bronze tables, but force a reload
--
COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/iotv1`     FROM 'dbfs:/databricks-datasets/iot'                    FILEFORMAT = JSON COPY_OPTIONS ('force' = 'true');
COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/userv1`    FROM 'dbfs:/databricks-datasets/iot-stream/data-user'   FILEFORMAT = CSV  FORMAT_OPTIONS('header'='true', 'inferSchema' = 'true') COPY_OPTIONS ('force' = 'true');
COPY INTO delta.`s3a://oetrta/dmoore/databases/bronze/devicev1`  FROM 'dbfs:/databricks-datasets/iot-stream/data-device' FILEFORMAT = JSON COPY_OPTIONS ('force' ='true');

-- COMMAND ----------

describe history iot;

-- COMMAND ----------

-- MAGIC %md ### Verify
-- MAGIC - duplicates

-- COMMAND ----------

-- Do the count
SELECT 'iot'    as name, COUNT(*) count FROM douglas_moore_bronze.iot
UNION ALL
SELECT 'user'   as name, COUNT(*) count FROM douglas_moore_bronze.user
UNION ALL
SELECT 'device' as name, COUNT(*) count FROM douglas_moore_bronze.device;

-- COMMAND ----------

-- MAGIC %md ## Extras
-- MAGIC - Select
-- MAGIC - Alter table, add column
-- MAGIC - Update
-- MAGIC - Delete

-- COMMAND ----------

SELECT * from douglas_moore_bronze.user;

-- COMMAND ----------

ALTER TABLE douglas_moore_bronze.user ADD COLUMNS (active int AFTER risk);

DESCRIBE user;

-- COMMAND ----------

SELECT * FROM douglas_moore_bronze.user;

-- COMMAND ----------

UPDATE douglas_moore_bronze.user set active = 1;

-- COMMAND ----------

SELECT * FROM douglas_moore_bronze.user;

-- COMMAND ----------

DELETE FROM douglas_moore_bronze.user where userid = 1

-- COMMAND ----------

SELECT * FROM douglas_moore_bronze.user ORDER BY userid ASC

-- COMMAND ----------

-- MAGIC %md ### Remove Duplicates

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.read.table('douglas_moore_bronze.user').dropDuplicates().write.mode('append').saveAsTable('douglas_moore_bronze.user')

-- COMMAND ----------

SELECT * FROM douglas_moore_bronze.user ORDER BY userid ASC

-- COMMAND ----------

-- MAGIC %md ## Cleanup

-- COMMAND ----------

--
-- reset
--
TRUNCATE TABLE iot;
TRUNCATE TABLE user;
TRUNCATE TABLE device;

DROP TABLE IF EXISTS douglas_moore_bronze.iot;
DROP TABLE IF EXISTS douglas_moore_bronze.user;
DROP TABLE IF EXISTS douglas_moore_bronze.device;

SHOW TABLES in douglas_moore_bronze;
DROP DATABASE IF EXISTS douglas_moore_bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('s3a://oetrta/dmoore/databases/bronze/iotv1', recurse=True)
-- MAGIC dbutils.fs.rm('s3a://oetrta/dmoore/databases/bronze/userv1', recurse=True)
-- MAGIC dbutils.fs.rm('s3a://oetrta/dmoore/databases/bronze/devicev1', recurse=True)

-- COMMAND ----------


