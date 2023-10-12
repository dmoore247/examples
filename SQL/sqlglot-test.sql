-- Databricks notebook source
-- MAGIC %pip install --quiet git+https://github.com/tobymao/sqlglot.git

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.library.restartPython()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import sqlglot

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql = """
-- MAGIC SET ANSI_SQL ON
-- MAGIC """
-- MAGIC sqlglot.transpile(tsql, read="tsql", write="duckdb")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql12 = """
-- MAGIC CREATE TABLE [dbo].[t_external_cmm_email](
-- MAGIC         [email] [varchar](255) NOT NULL,
-- MAGIC         [date_added] [datetime] NULL,
-- MAGIC  CONSTRAINT [UN_t_external_cmm_email] PRIMARY KEY CLUSTERED
-- MAGIC (
-- MAGIC         [email] ASC
-- MAGIC )
-- MAGIC ) ON [PRIMARY]
-- MAGIC """
-- MAGIC sqlglot.parse(tsql12, read="tsql")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC # parses
-- MAGIC tsql0 = """CREATE TABLE [dbo].[t_cmm_dim_geo](
-- MAGIC 	[zip_cd_mkey] [int] NOT NULL,
-- MAGIC 	[zip_cd] [varchar](5) NULL,
-- MAGIC  CONSTRAINT [pk_t_cmm_dim_geo__zip_cd_mkey] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC 	[zip_cd_mkey]
-- MAGIC )
-- MAGIC ) ON [PRIMARY]
-- MAGIC """
-- MAGIC sqlglot.parse(sql=tsql0, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # fails on `ASC`
-- MAGIC from sqlglot.errors import ParseError
-- MAGIC tsql1 = """CREATE TABLE [dbo].[t_cmm_dim_geo](
-- MAGIC 	[zip_cd_mkey] [int] NOT NULL,
-- MAGIC 	[zip_cd] [varchar](5) NULL,
-- MAGIC  CONSTRAINT [pk_t_cmm_dim_geo__zip_cd_mkey] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC 	[zip_cd_mkey] ASC
-- MAGIC )
-- MAGIC ) ON [PRIMARY]
-- MAGIC """
-- MAGIC try:
-- MAGIC 	sqlglot.parse(sql=tsql1, read="tsql")
-- MAGIC except ParseError as e:
-- MAGIC     print(tsql1, '\n'+'---'*50+'\n', e)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # fails on the `WITH` clause
-- MAGIC tsql2 = """CREATE TABLE [dbo].[t_cmm_dim_geo](
-- MAGIC 	[zip_cd_mkey] [int] NOT NULL,
-- MAGIC 	[zip_cd] [varchar](5) NULL,
-- MAGIC  CONSTRAINT [pk_t_cmm_dim_geo__zip_cd_mkey] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC 	[zip_cd_mkey]
-- MAGIC ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
-- MAGIC ) ON [PRIMARY]
-- MAGIC """
-- MAGIC sqlglot.parse(sql=tsql2, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # fails on the `ON [PRIMARY]` clause
-- MAGIC tsql3 = """CREATE TABLE [dbo].[t_cmm_dim_geo](
-- MAGIC 	[zip_cd_mkey] [int] NOT NULL,
-- MAGIC 	[zip_cd] [varchar](5) NULL,
-- MAGIC  CONSTRAINT [pk_t_cmm_dim_geo__zip_cd_mkey] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC 	[zip_cd_mkey]
-- MAGIC ) ON [PRIMARY]
-- MAGIC ) ON [PRIMARY]
-- MAGIC """
-- MAGIC sqlglot.parse(sql=tsql3, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # fails
-- MAGIC tsql4 = """CREATE TABLE [dbo].[mytable](
-- MAGIC 	[zip_cd_mkey] [int] NOT NULL,
-- MAGIC 	[zip_cd] [varchar](5) NULL
-- MAGIC   CONSTRAINT [pk_mytable] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC 	[zip_cd_mkey] ASC
-- MAGIC ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, 
-- MAGIC        ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
-- MAGIC ) ON [PRIMARY]
-- MAGIC """
-- MAGIC sqlglot.parse(sql=tsql4, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql5 = """
-- MAGIC CREATE TABLE [dbo].[mtm_UserGroupIdentifier](
-- MAGIC 	[usergroup_id] [int] NOT NULL,
-- MAGIC 	[identifier_id] [int] NOT NULL,
-- MAGIC  CONSTRAINT [PK_mtm_UserGroupIdentifier] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC 	[usergroup_id],
-- MAGIC 	[identifier_id]
-- MAGIC )
-- MAGIC ) ON [PRIMARY]
-- MAGIC """
-- MAGIC sqlglot.parse(sql=tsql5, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql6 = """
-- MAGIC CREATE TABLE [dbo].[t_external_cmm_email](
-- MAGIC 	[id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
-- MAGIC 	[email] [varchar](255) NOT NULL,
-- MAGIC 	[date_added] [datetime] NULL,
-- MAGIC  CONSTRAINT [PK_t_external_cmm_email] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC 	[id] ASC
-- MAGIC )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
-- MAGIC  CONSTRAINT [UN_t_external_cmm_email] UNIQUE NONCLUSTERED 
-- MAGIC (
-- MAGIC 	[email] ASC
-- MAGIC )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
-- MAGIC ) ON [PRIMARY]"""
-- MAGIC sqlglot.parse(sql=tsql5, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql6 = """
-- MAGIC CREATE TABLE [dbo].[mytable](
-- MAGIC 	[id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
-- MAGIC 	[mycolumn] [varchar](500) NULL
-- MAGIC  CONSTRAINT [PK__mytable_1] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC 	[id] ASC
-- MAGIC )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
-- MAGIC ) ON [PRIMARY]
-- MAGIC """
-- MAGIC sqlglot.parse(sql=tsql6, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql7 = """CREATE INDEX idxN_user_id on #internal(user_id)"""
-- MAGIC sqlglot.parse(sql=tsql7, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql8 = """CREATE TABLE [dbo].[mytable](
-- MAGIC [user_id] [int] NOT NULL
-- MAGIC CONSTRAINT [pk_mytable__user_id] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC [user_id] ASC
-- MAGIC )WITH (PAD_INDEX = ON, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = OFF, ALLOW_PAGE_LOCKS = OFF) ON [PRIMARY]
-- MAGIC ) ON [PRIMARY]"""
-- MAGIC sqlglot.parse(sql=tsql8, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql9 = """CREATE TABLE [dbo].[mytable](
-- MAGIC [user_id] [int] NOT NULL
-- MAGIC CONSTRAINT [pk_mytable__user_id] PRIMARY KEY CLUSTERED 
-- MAGIC (
-- MAGIC [user_id] DESC
-- MAGIC )WITH (
-- MAGIC     COMPRESSION_DELAY = 10 MINUTES,
-- MAGIC     DATA_COMPRESSION = PAGE
-- MAGIC     ) ON [PRIMARY]
-- MAGIC ) ON [PRIMARY]"""
-- MAGIC sqlglot.parse(sql=tsql9, read="tsql")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tsql10 = """
-- MAGIC CREATE CLUSTERED COLUMNSTORE INDEX cci ON Sales.OrderLines
-- MAGIC WITH ( COMPRESSION_DELAY = 10 MINUTES );
-- MAGIC """
-- MAGIC sqlglot.parse(sql=tsql10, read="tsql")

-- COMMAND ----------

-- MAGIC %md # Report out on last sql transpilation run

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * 
-- MAGIC from douglas_moore.sqlglot.project1
-- MAGIC order by insert_dt desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(1) cnt, file_path, isnotnull(plan)
-- MAGIC from douglas_moore.sqlglot.project1
-- MAGIC group by 2,3
-- MAGIC order by 1 desc

-- COMMAND ----------

select count(1) cnt, statement_type, strategy, error_class
from douglas_moore.sqlglot.project1
group by 2,3, 4
order by 1 desc

-- COMMAND ----------

select * from douglas_moore.sqlglot.project1
where strategy = 'File sqlglot issue'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from douglas_moore.sqlglot.project1
-- MAGIC where statement_type = 'create'

-- COMMAND ----------

select * from douglas_moore.sqlglot.project1
WHERE statement_type = 'CREATE TABLE'
AND error_class = 'PARSE_SYNTAX_ERROR'

-- COMMAND ----------

select * from douglas_moore.sqlglot.project1


-- COMMAND ----------

use douglas_moore.dbo;

CREATE OR REPLACE TABLE `dbo`.`T_Users` (
  `id` BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) ,  CONSTRAINT `PK_T_Users` PRIMARY KEY (`id`))

-- COMMAND ----------

CREATE TABLE douglas_moore.dbo.pkconstraint_table (`zip_cd` VARCHAR(5) NOT NULL, CONSTRAINT `pk_mytable2` PRIMARY KEY (`zip_cd`))

-- COMMAND ----------

SET $count = 10

-- COMMAND ----------

SET COUNT = 10

-- COMMAND ----------


