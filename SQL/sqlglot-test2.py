# Databricks notebook source
# MAGIC %pip install --quiet git+https://github.com/tobymao/sqlglot.git

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sqlglot

# COMMAND ----------

path = "/Workspace/Repos/douglas.moore@databricks.com/examples/SQL/tests/resources/tsql/project1/*.sql"


# COMMAND ----------

tsql= """
CREATE TABLE [dbo].[mytable](
	[col1] [varchar](16) NOT NULL,
	[created] [date].    NOT NULL,
 CONSTRAINT [PK_mytable] PRIMARY KEY NONCLUSTERED 
(
	[request_id] ASC
)
) ON [ps_trr_yearmonth]([created])
"""
import sqlglot
sqlglot.parse(tsql, read="tsql")

# COMMAND ----------


