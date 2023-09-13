# Databricks notebook source
# MAGIC %pip install --quiet git+https://github.com/tobymao/sqlglot.git

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

path = "/Workspace/Repos/douglas.moore@databricks.com/examples/SQL/tests/resources/tsql/project1/*.sql"

# COMMAND ----------

from control import run_all

# COMMAND ----------

run_all(path=path, table_name="douglas_moore.sqlglot.project1")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table douglas_moore.sqlglot.mckesson7

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from douglas_moore.sqlglot.mckesson7

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) cnt, file_path, error_class
# MAGIC FROM douglas_moore.sqlglot.mckesson7 a
# MAGIC GROUP BY file_path, error_class 
# MAGIC ORDER BY cnt DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from douglas_moore.sqlglot.mckesson7
# MAGIC where error_class is null

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(1) statement_cnt
# MAGIC FROM douglas_moore.sqlglot.mckesson7
# MAGIC ORDER BY statement_cnt DESC

# COMMAND ----------


