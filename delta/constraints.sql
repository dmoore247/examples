-- Databricks notebook source
DROP TABLE IF EXISTS default.people10m;

CREATE TABLE default.people10m (
  id INT,
  firstName STRING NOT NULL,
  middleName STRING,
  lastName STRING NOT NULL,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) USING DELTA;

ALTER TABLE default.people10m ADD CONSTRAINT dateWithinRange1 CHECK (birthDate > '1900-01-01' AND salary > 5000) ;
ALTER TABLE default.people10m ADD CONSTRAINT dateWithinRange2 CHECK (birthDate > '1950-01-01' AND salary > 50000) ;
--ALTER TABLE default.people10m DROP CONSTRAINT dateWithinRange;

-- COMMAND ----------

describe detail default.people10m

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ```
-- MAGIC INSERT INTO [ TABLE ] table_identifier [ partition_spec ] [ ( column_list ) ]
-- MAGIC     { VALUES ( { value | NULL } [ , ... ] ) [ , ( ... ) ] | query }
-- MAGIC     ```

-- COMMAND ----------

INSERT INTO default.people10m (id, firstName, middleName, lastName, gender, birthDate, ssn, salary)
VALUES ( 1, "Douglas", null, "Moore", "m", '1901-01-02', "123-45-678", 101499)

-- COMMAND ----------

INSERT INTO default.people10m (id, firstName, middleName, lastName, gender, birthDate, ssn, salary)
VALUES ( 1, "Doug", "jr", "Moore", "m", '1901-01-02', "123-45-678", 199)

-- COMMAND ----------

select * from default.people10m

-- COMMAND ----------

INSERT INTO default.people10m (id, firstName, middleName, lastName, gender, birthDate, ssn, salary)
VALUES ( 1, "Joan", "o", null, "m", '1901-01-02', "123-45-678", 101499)

-- COMMAND ----------

INSERT INTO default.people10m (id, firstName, lastName, gender, birthDate, ssn, salary)
VALUES ( 3, "Joan", "Ark", "m", '1901-01-02', "123-45-678", 101499)

-- COMMAND ----------

INSERT INTO default.people10m (id, firstName, middleName, lastName, gender, birthDate, ssn)
VALUES ( 4, "Joan", "o", "Ark", "m", '1901-01-02', "123-45-678")

-- COMMAND ----------


