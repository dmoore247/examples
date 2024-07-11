# Databricks notebook source
# MAGIC %pip install git+https://github.com/dmoore247/sqlglot.git@feat/update_from

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sqlglot

# COMMAND ----------

# MAGIC %sql
# MAGIC -- summarize results
# MAGIC SELECT count(1) count, statement_type, strategy, error_class, cast(insert_dt as DATE)
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC GROUP BY 2,3,4,5
# MAGIC ORDER BY 1 DESC, 2

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from douglas_moore.sqlglot.project1
# MAGIC where statement_type = 'UPDATE FROM'
# MAGIC and context is null

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- tsql
# MAGIC UPDATE ru_new 
# MAGIC SET user_category = CASE WHEN user_type LIKE '%Pharmacy%' or user_type = '8' --prod bug 2015/10/21
# MAGIC THEN 'Pharmacy' ELSE 'Physician' END
# MAGIC FROM t_reporting_user_merge ru_new
# MAGIC WHERE ru_new.user_category IS NULL
# MAGIC

# COMMAND ----------

tsql = """
UPDATE ru_new 
SET user_category = CASE WHEN user_type LIKE '%Pharmacy%' or user_type = '8' --prod bug 2015/10/21
THEN 'Pharmacy' ELSE 'Physician' END
FROM t_reporting_user_merge ru_new
WHERE ru_new.user_category IS NULL
"""
r = sqlglot.transpile(tsql, read='tsql', write='databricks')
sqlglot.parse_one(tsql,read='tsql')

# COMMAND ----------

# MAGIC %md ### Statezips example

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE table statezips (state STRING, zip STRING, geo STRING);
# MAGIC
# MAGIC CREATE OR REPLACE table zipcodes (state STRING, zipcode STRING);
# MAGIC
# MAGIC MERGE INTO statezips AS sz
# MAGIC USING zipcodes AS z 
# MAGIC     ON LEFT(sz.zip, 5) = z.zipcode
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   sz.state = z.state;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- migrated
# MAGIC MERGE INTO statezips target 
# MAGIC USING (
# MAGIC     SELECT z.state FROM zipcodes AS z
# MAGIC ) AS src 
# MAGIC ON target.state = src.state 
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE 
# MAGIC SET target.state = src.state

# COMMAND ----------

# MAGIC %md TSQL
# MAGIC
# MAGIC ```sql
# MAGIC -- tsql
# MAGIC UPDATE ru_new 
# MAGIC SET geo = CASE WHEN geo LIKE '%west%' or zip = '0' THEN 'West' ELSE 'Midwest' END
# MAGIC FROM statezips ru_new
# MAGIC WHERE ru_new.geo IS NULL;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- UPDATE table_name [table_alias]
# MAGIC --    SET  { { column_name | field_name }  = [ expr | DEFAULT } [, ...]
# MAGIC --    [WHERE clause]
# MAGIC
# MAGIC UPDATE statezips r_new
# MAGIC SET r_new.geo = CASE WHEN r_new.geo LIKE '%west%' or zip = '0' --prod bug 2015/10/21
# MAGIC THEN 'West' ELSE 'Midwest' END
# MAGIC WHERE r_new.geo IS NULL;
# MAGIC  

# COMMAND ----------

# MAGIC %md # MERGE INTO documentation
# MAGIC ```sql
# MAGIC MERGE INTO target_table_name [target_alias]
# MAGIC    USING source_table_reference [source_alias]
# MAGIC    ON merge_condition
# MAGIC    { WHEN MATCHED [ AND matched_condition ] THEN matched_action |
# MAGIC      WHEN NOT MATCHED [BY TARGET] [ AND not_matched_condition ] THEN not_matched_action |
# MAGIC      WHEN NOT MATCHED BY SOURCE [ AND not_matched_by_source_condition ] THEN not_matched_by_source_action } [...]
# MAGIC
# MAGIC matched_action
# MAGIC  { DELETE |
# MAGIC    UPDATE SET * |
# MAGIC    UPDATE SET { column = { expr | DEFAULT } } [, ...] }
# MAGIC
# MAGIC not_matched_action
# MAGIC  { INSERT * |
# MAGIC    INSERT (column1 [, ...] ) VALUES ( expr | DEFAULT ] [, ...] )
# MAGIC
# MAGIC not_matched_by_source_action
# MAGIC  { DELETE |
# MAGIC    UPDATE SET { column = { expr | DEFAULT } } [, ...] }
# MAGIC ```

# COMMAND ----------

# MAGIC %md ### Code Simplification

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog douglas_moore;
# MAGIC use database dbo;
# MAGIC
# MAGIC MERGE INTO statezips USING (SELECT z.state FROM zipcodes AS z) AS src ON state = src.state WHEN MATCHED THEN UPDATE SET state = src.state

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC FROM douglas_moore.sqlglot.project1
# MAGIC where statement_type = "CREATE TABLE"
# MAGIC and error_class is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog douglas_moore;
# MAGIC CREATE TABLE `dbo`.`t_cmm_dim_geo` (
# MAGIC   `zip_cd_mkey` INT NOT NULL, `zip_cd` VARCHAR(5), 
# MAGIC   `zip_tp_cd` CHAR(1), `zip_tp_nm_tx` VARCHAR(18), 
# MAGIC   `city_tp_cd` CHAR(1), `city_tp_nm_tx` VARCHAR(26), `city_nm_tx` VARCHAR(64), `state_fips_cd` VARCHAR(2), `state_cd` VARCHAR(2), `state_nm_tx` VARCHAR(64), `msa_cd` VARCHAR(4), `msa_tp_cd` VARCHAR(5), `msa_nm_tx` VARCHAR(64), 
# MAGIC   `cbsa_cd` VARCHAR(5), `cbsa_tp_cd` VARCHAR(3), `cbsa_tp_nm` VARCHAR(12), `cbsa_nm_tx` VARCHAR(64), `csa_cd` VARCHAR(3), `csa_nm_tx` VARCHAR(64), `dvsn_cd` VARCHAR(5), `dvsn_nm_tx` VARCHAR(64), `cnty_fips_cd` VARCHAR(5), `cnty_nm_tx` VARCHAR(64), `area_cd` VARCHAR(16), `tm_zn_cd` VARCHAR(16), `utc_cd` INT, `dst_ind` CHAR(1), `lat_num` VARCHAR(11), `lon_num` VARCHAR(11), `version` INT, `date_from` TIMESTAMP, `date_to` TIMESTAMP,
# MAGIC   `safe_harbor_zip_cd` STRING GENERATED ALWAYS AS (
# MAGIC     (CASE WHEN LEFT(`zip_cd`, (3)) = '893' OR LEFT(`zip_cd`, (3)) = '890' OR LEFT(`zip_cd`, (3)) = '884' OR LEFT(`zip_cd`, (3)) = '879' OR LEFT(`zip_cd`, (3)) = '878' OR LEFT(`zip_cd`, (3)) = '831' OR LEFT(`zip_cd`, (3)) = '830' OR LEFT(`zip_cd`, (3)) = '823' OR LEFT(`zip_cd`, (3)) = '821' OR LEFT(`zip_cd`, (3)) = '790' OR LEFT(`zip_cd`, (3)) = '692' OR LEFT(`zip_cd`, (3)) = '556' OR LEFT(`zip_cd`, (3)) = '203' OR LEFT(`zip_cd`, (3)) = '102' OR LEFT(`zip_cd`, (3)) = '063' OR LEFT(`zip_cd`, (3)) = '059' OR LEFT(`zip_cd`, (3)) = '036' THEN LEFT(`zip_cd`, (3)) + 'XX' ELSE `zip_cd` END)
# MAGIC     ), CONSTRAINT `pk_t_cmm_dim_geo__zip_cd_mkey` PRIMARY KEY (`zip_cd_mkey`)
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS zipcodes;
# MAGIC EXPLAIN COST CREATE TEMPORARY TABLE zipcodes (zipcode VARCHAR(5), State VARCHAR(2)) USING PARQUET;

# COMMAND ----------

CREATE TABLE [dbo].[t_cmm_dim_geo](
	[zip_cd_mkey] [int] NOT NULL,
	[zip_cd] [varchar](5) NULL,
	[date_to] [datetime] NULL,
	[safe_harbor_zip_cd]  AS (case when left([zip_cd],(3))='893' OR left([zip_cd],(3))='890' OR left([zip_cd],(3))='884' OR left([zip_cd],(3))='879' OR left([zip_cd],(3))='878' OR left([zip_cd],(3))='831' OR left([zip_cd],(3))='830' OR left([zip_cd],(3))='823' OR left([zip_cd],(3))='821' OR left([zip_cd],(3))='790' OR left([zip_cd],(3))='692' OR left([zip_cd],(3))='556' OR left([zip_cd],(3))='203' OR left([zip_cd],(3))='102' OR left([zip_cd],(3))='063' OR left([zip_cd],(3))='059' OR left([zip_cd],(3))='036' then left([zip_cd],(3))+'XX' else [zip_cd] end) PERSISTED,
 CONSTRAINT [pk_t_cmm_dim_geo__zip_cd_mkey] PRIMARY KEY CLUSTERED 
(
	[zip_cd_mkey] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE `dbo`.`t_cmm_dim_geo` (`zip_cd_mkey` INT NOT NULL, `zip_cd` VARCHAR(5), 
# MAGIC `date_to` TIMESTAMP, 
# MAGIC `safe_harbor_zip_cd` AS (
# MAGIC   CASE WHEN LEFT(`zip_cd`, (3)) = '893' OR LEFT(`zip_cd`, (3)) = '890' OR LEFT(`zip_cd`, (3)) = '884' OR LEFT(`zip_cd`, (3)) = '879' OR LEFT(`zip_cd`, (3)) = '878' OR LEFT(`zip_cd`, (3)) = '831' OR LEFT(`zip_cd`, (3)) = '830' OR LEFT(`zip_cd`, (3)) = '823' OR LEFT(`zip_cd`, (3)) = '821' OR LEFT(`zip_cd`, (3)) = '790' OR LEFT(`zip_cd`, (3)) = '692' OR LEFT(`zip_cd`, (3)) = '556' OR LEFT(`zip_cd`, (3)) = '203' OR LEFT(`zip_cd`, (3)) = '102' OR LEFT(`zip_cd`, (3)) = '063' OR LEFT(`zip_cd`, (3)) = '059' OR LEFT(`zip_cd`, (3)) = '036' THEN LEFT(`zip_cd`, (3)) + 'XX' ELSE `zip_cd` END) PERSISTED,
# MAGIC CONSTRAINT `pk_t_cmm_dim_geo__zip_cd_mkey` PRIMARY KEY (`zip_cd_mkey`)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE x (id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 3 INCREMENT BY 7))

# COMMAND ----------

ParseException("\n[PARSE_SYNTAX_ERROR] Syntax error at or near 'AS'.(line 1, pos 62)\n\n== SQL ==\nexplain COST CREATE TABLE `dbo`.`T_Users` (`id` INT GENERATED AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, `username` VARCHAR(64) NOT NULL, `password` VARCHAR(64) NOT NULL, `legacy_pw` VARCHAR(64), `display_name` VARCHAR(255), `organization_name` VARCHAR(255), `email` VARCHAR(255), `is_prescriber` INT, `prescriber_street` VARCHAR(255), `prescriber_city` VARCHAR(255), `prescriber_state` VARCHAR(2), `prescriber_zip` VARCHAR(16), `office_phone` VARCHAR(32), `office_fax` VARCHAR(32), `office_contact` VARCHAR(255), `npi` VARCHAR(255), `created_on` TIMESTAMP, `is_deleted` BOOLEAN, `druglist` VARCHAR(8000), `deleted` BOOLEAN NOT NULL, `deleted_on` TIMESTAMP, `role` SMALLINT, `eula_agree` BOOLEAN NOT NULL, `notify_me` TINYINT, `last_notification_dismissed` TIMESTAMP NOT NULL, `last_password_attempt` TIMESTAMP, `password_attempt_count` INT, `user_type` INT, `referral_source` VARCHAR(500), `customer_status` VARCHAR(50), `show_history` TINYINT NOT NULL, `user_agent` VARCHAR(500), `requests_per_week` VARCHAR(50), `reminder_email` INT NOT NULL, `form_search` BOOLEAN NOT NULL, `ncpdp_id` VARCHAR(7), `autofill_address` BOOLEAN NOT NULL, `cmm_usage` VARCHAR(500), `org_description` VARCHAR(500), `prescriber_specialty` VARCHAR(50), `rep_territory` VARCHAR(50), `signup_complete` BOOLEAN NOT NULL, `number_beds` VARCHAR(50), `referral_type` VARCHAR(25), `pa_lead` BOOLEAN NOT NULL, `ba_agree` BOOLEAN NOT NULL, `no_offers` BOOLEAN NOT NULL, `olark_chat` BOOLEAN NOT NULL, `software_vendor` VARCHAR(50), `auto_notify` TINYINT, `userbranding_id` INT, `coversheet_only` TINYINT, `web_iprange` VARCHAR(255), `claims_iprange` VARCHAR(255), `power_dashboard` BOOLEAN NOT NULL, `phone_to_fax` BOOLEAN NOT NULL, `renotify_on_dup` BOOLEAN NOT NULL, `defer_form_choice` BOOLEAN NOT NULL, `account_id` INT, `renotify_interval` INT, `reminder_interval` INT, `faxaction_interval` INT, `is_confirmed` BOOLEAN, `confirmation_token` VARCHAR(32), `confirmation_create_date` T...")

# COMMAND ----------

import sqlglot

# COMMAND ----------

sql = '''CREATE TABLE [dbo].[T_Users](
	[id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[username] [varchar](64) NOT NULL,
	[password] [varchar](64) NOT NULL,
	[legacy_pw] [varchar](64) NULL,
	[display_name] [varchar](255) NULL,
	[organization_name] [varchar](255) NULL,
	[email] [varchar](255) NULL,
	[is_prescriber] [int] NULL,
	[prescriber_street] [varchar](255) NULL,
	[prescriber_city] [varchar](255) NULL,
	[prescriber_state] [varchar](2) NULL,
	[prescriber_zip] [varchar](16) NULL,
	[office_phone] [varchar](32) NULL,
	[office_fax] [varchar](32) NULL,
	[office_contact] [varchar](255) NULL,
	[npi] [varchar](255) NULL,
	[created_on] [datetime] NULL,
	[is_deleted] [bit] NULL,
	[druglist] [varchar](8000) NULL,
	[deleted] [bit] NOT NULL,
	[deleted_on] [datetime] NULL,
	[role] [smallint] NULL,
	[eula_agree] [bit] NOT NULL,
	[notify_me] [tinyint] NULL,
	[last_notification_dismissed] [datetime] NOT NULL,
	[last_password_attempt] [datetime] NULL,
	[password_attempt_count] [int] NULL,
	[user_type] [int] NULL,
	[referral_source] [varchar](500) NULL,
	[customer_status] [varchar](50) NULL,
	[show_history] [tinyint] NOT NULL,
	[user_agent] [varchar](500) NULL,
	[requests_per_week] [varchar](50) NULL,
	[reminder_email] [int] NOT NULL,
	[form_search] [bit] NOT NULL,
	[ncpdp_id] [varchar](7) NULL,
	[autofill_address] [bit] NOT NULL,
	[cmm_usage] [varchar](500) NULL,
	[org_description] [varchar](500) NULL,
	[prescriber_specialty] [varchar](50) NULL,
	[rep_territory] [varchar](50) NULL,
	[signup_complete] [bit] NOT NULL,
	[number_beds] [varchar](50) NULL,
	[referral_type] [varchar](25) NULL,
	[pa_lead] [bit] NOT NULL,
	[ba_agree] [bit] NOT NULL,
	[no_offers] [bit] NOT NULL,
	[olark_chat] [bit] NOT NULL,
	[software_vendor] [varchar](50) NULL,
	[auto_notify] [tinyint] NULL,
	[userbranding_id] [int] NULL,
	[coversheet_only] [tinyint] NULL,
	[web_iprange] [varchar](255) NULL,
	[claims_iprange] [varchar](255) NULL,
	[power_dashboard] [bit] NOT NULL,
	[phone_to_fax] [bit] NOT NULL,
	[renotify_on_dup] [bit] NOT NULL,
	[defer_form_choice] [bit] NOT NULL,
	[account_id] [int] NULL,
	[renotify_interval] [int] NULL,
	[reminder_interval] [int] NULL,
	[faxaction_interval] [int] NULL,
	[is_confirmed] [bit] NULL,
	[confirmation_token] [varchar](32) NULL,
	[confirmation_create_date] [datetime] NULL,
	[fax_notification] [varchar](50) NULL,
	[fax_in_intermediary] [bit] NOT NULL,
	[locked] [bit] NOT NULL,
	[job_title] [varchar](50) NULL,
	[password_reset_token] [varchar](36) NULL,
	[password_reset_expires_on] [datetime] NULL,
	[bcrypt_password] [varchar](64) NULL,
 CONSTRAINT [PK_T_Users] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]'''

# COMMAND ----------

r = sqlglot.transpile(sql=sql, read='tsql',write='databricks')

# COMMAND ----------

r[0]

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC CREATE TABLE `dbo`.`T_Users` (`id` BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, `username` VARCHAR(64) NOT NULL, `password` VARCHAR(64) NOT NULL, `legacy_pw` VARCHAR(64), `display_name` VARCHAR(255), `organization_name` VARCHAR(255), `email` VARCHAR(255), `is_prescriber` INT, `prescriber_street` VARCHAR(255), `prescriber_city` VARCHAR(255), `prescriber_state` VARCHAR(2), `prescriber_zip` VARCHAR(16), `office_phone` VARCHAR(32), `office_fax` VARCHAR(32), `office_contact` VARCHAR(255), `npi` VARCHAR(255), `created_on` TIMESTAMP, `is_deleted` BOOLEAN, `druglist` VARCHAR(8000), `deleted` BOOLEAN NOT NULL, `deleted_on` TIMESTAMP, `role` SMALLINT, `eula_agree` BOOLEAN NOT NULL, `notify_me` TINYINT, `last_notification_dismissed` TIMESTAMP NOT NULL, `last_password_attempt` TIMESTAMP, `password_attempt_count` INT, `user_type` INT, `referral_source` VARCHAR(500), `customer_status` VARCHAR(50), `show_history` TINYINT NOT NULL, `user_agent` VARCHAR(500), `requests_per_week` VARCHAR(50), `reminder_email` INT NOT NULL, `form_search` BOOLEAN NOT NULL, `ncpdp_id` VARCHAR(7), `autofill_address` BOOLEAN NOT NULL, `cmm_usage` VARCHAR(500), `org_description` VARCHAR(500), `prescriber_specialty` VARCHAR(50), `rep_territory` VARCHAR(50), `signup_complete` BOOLEAN NOT NULL, `number_beds` VARCHAR(50), `referral_type` VARCHAR(25), `pa_lead` BOOLEAN NOT NULL, `ba_agree` BOOLEAN NOT NULL, `no_offers` BOOLEAN NOT NULL, `olark_chat` BOOLEAN NOT NULL, `software_vendor` VARCHAR(50), `auto_notify` TINYINT, `userbranding_id` INT, `coversheet_only` TINYINT, `web_iprange` VARCHAR(255), `claims_iprange` VARCHAR(255), `power_dashboard` BOOLEAN NOT NULL, `phone_to_fax` BOOLEAN NOT NULL, `renotify_on_dup` BOOLEAN NOT NULL, `defer_form_choice` BOOLEAN NOT NULL, `account_id` INT, `renotify_interval` INT, `reminder_interval` INT, `faxaction_interval` INT, `is_confirmed` BOOLEAN, `confirmation_token` VARCHAR(32), `confirmation_create_date` TIMESTAMP, `fax_notification` VARCHAR(50), `fax_in_intermediary` BOOLEAN NOT NULL, `locked` BOOLEAN NOT NULL, `job_title` VARCHAR(50), `password_reset_token` VARCHAR(36), `password_reset_expires_on` TIMESTAMP, `bcrypt_password` VARCHAR(64), CONSTRAINT `PK_T_Users` PRIMARY KEY (`id`))

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC CREATE TABLE `dbo`.`T_Users` (`id` BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, `username` VARCHAR(64) NOT NULL, `password` VARCHAR(64) NOT NULL, `legacy_pw` VARCHAR(64), `display_name` VARCHAR(255), `organization_name` VARCHAR(255), `email` VARCHAR(255), `is_prescriber` INT, `prescriber_street` VARCHAR(255), `prescriber_city` VARCHAR(255), `prescriber_state` VARCHAR(2), `prescriber_zip` VARCHAR(16), `office_phone` VARCHAR(32), `office_fax` VARCHAR(32), `office_contact` VARCHAR(255), `npi` VARCHAR(255), `created_on` TIMESTAMP, `is_deleted` BOOLEAN, `druglist` VARCHAR(8000), `deleted` BOOLEAN NOT NULL, `deleted_on` TIMESTAMP, `role` SMALLINT, `eula_agree` BOOLEAN NOT NULL, `notify_me` TINYINT, `last_notification_dismissed` TIMESTAMP NOT NULL, `last_password_attempt` TIMESTAMP, `password_attempt_count` INT, `user_type` INT, `referral_source` VARCHAR(500), `customer_status` VARCHAR(50), `show_history` TINYINT NOT NULL, `user_agent` VARCHAR(500), `requests_per_week` VARCHAR(50), `reminder_email` INT NOT NULL, `form_search` BOOLEAN NOT NULL, `ncpdp_id` VARCHAR(7), `autofill_address` BOOLEAN NOT NULL, `cmm_usage` VARCHAR(500), `org_description` VARCHAR(500), `prescriber_specialty` VARCHAR(50), `rep_territory` VARCHAR(50), `signup_complete` BOOLEAN NOT NULL, `number_beds` VARCHAR(50), `referral_type` VARCHAR(25), `pa_lead` BOOLEAN NOT NULL, `ba_agree` BOOLEAN NOT NULL, `no_offers` BOOLEAN NOT NULL, `olark_chat` BOOLEAN NOT NULL, `software_vendor` VARCHAR(50), `auto_notify` TINYINT, `userbranding_id` INT, `coversheet_only` TINYINT, `web_iprange` VARCHAR(255), `claims_iprange` VARCHAR(255), `power_dashboard` BOOLEAN NOT NULL, `phone_to_fax` BOOLEAN NOT NULL, `renotify_on_dup` BOOLEAN NOT NULL, `defer_form_choice` BOOLEAN NOT NULL, `account_id` INT, `renotify_interval` INT, `reminder_interval` INT, `faxaction_interval` INT, `is_confirmed` BOOLEAN, `confirmation_token` VARCHAR(32), `confirmation_create_date` TIMESTAMP, `fax_notification` VARCHAR(50), `fax_in_intermediary` BOOLEAN NOT NULL, `locked` BOOLEAN NOT NULL, `job_title` VARCHAR(50), `password_reset_token` VARCHAR(36), `password_reset_expires_on` TIMESTAMP, `bcrypt_password` VARCHAR(64), CONSTRAINT `PK_T_Users` PRIMARY KEY (`id`))

# COMMAND ----------

# MAGIC %md ## Create table

# COMMAND ----------

import sqlglot


# COMMAND ----------

tsql = """CREATE TABLE dbo.Products
    (
       ProductID int IDENTITY (1,1) NOT NULL
       , QtyAvailable smallint
       , SaleDate date
       , UnitPrice money
       , InventoryValue AS QtyAvailable * UnitPrice
       , yr AS YEAR(SaleDate)
     );"""

# COMMAND ----------

dbx_sql = sqlglot.transpile(tsql, read='tsql',write='databricks')[0]
dbx_sql

# COMMAND ----------

CREATE TABLE dbo.Products (
    ProductID INT GENERATED AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, 
    QtyAvailable SMALLINT, 
    SaleDate DATE, 
    UnitPrice DECIMAL(15, 4), 
    InventoryValue AS QtyAvailable * UnitPrice, 
    yr AS YEAR(SaleDate)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog douglas_moore;
# MAGIC
# MAGIC CREATE TABLE dbo.Products (
# MAGIC   ProductID BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) NOT NULL, 
# MAGIC   QtyAvailable SMALLINT, 
# MAGIC   SaleDate DATE, 
# MAGIC   UnitPrice DECIMAL(15, 4), 
# MAGIC   InventoryValue DECIMAL(15, 4) GENERATED ALWAYS AS (CAST(QtyAvailable * UnitPrice AS DECIMAL(15,4))) , 
# MAGIC   yr INT GENERATED ALWAYS AS (YEAR(SaleDate))
# MAGIC %  )

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog douglas_moore;
# MAGIC CREATE OR REPLACE TABLE dbo.tbl (id BIGINT NOT NULL GENERATED always AS IDENTITY (START WITH 10 INCREMENT BY 1) PRIMARY KEY)
# MAGIC

# COMMAND ----------

# MAGIC %sql describe extended dbo.tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table dbo.tbl;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- tsql
# MAGIC UPDATE ru_new SET state = zc.state
# MAGIC     FROM t_reporting_user_merge ru_new
# MAGIC     JOIN zipcodes zc
# MAGIC     ON LEFT(ru_new.zip,5) = zc.zipcode

# COMMAND ----------

# MAGIC %sql
# MAGIC USE catalog douglas_moore;
# MAGIC USE SCHEMA dbo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  zipcodes(state char(2), zipcode char(5));
# MAGIC CREATE TABLE IF NOT EXISTS tmerge(id INT, state char(2), zip char(9));

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO tmerge VALUES   (1, null, '01890001');
# MAGIC INSERT INTO tmerge VALUES   (2, null, '01890002');
# MAGIC INSERT INTO tmerge VALUES   (3, null, '01890003');

# COMMAND ----------

# MAGIC %sql select * from tmerge;

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN COST
# MAGIC UPDATE tmerge
# MAGIC SET state = (SELECT state
# MAGIC              FROM zipcodes
# MAGIC              WHERE LEFT(tmerge.zip, 5) = zipcodes.zipcode)

# COMMAND ----------

# MAGIC %md ## UPDATE FROM
# MAGIC ```sql
# MAGIC -- tsql
# MAGIC UPDATE ru_new SET state = zc.state
# MAGIC     FROM t_reporting_user_merge ru_new
# MAGIC     JOIN zipcodes zc
# MAGIC     ON LEFT(ru_new.zip,5) = zc.zipcode
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC --- spark sql
# MAGIC MERGE INTO tmerge
# MAGIC USING zipcodes
# MAGIC ON LEFT(tmerge.zip, 5) = zipcodes.zipcode
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET tmerge.state = zipcodes.state
# MAGIC ```

# COMMAND ----------

import sqlglot

# COMMAND ----------

sqlglot.parse_one('''UPDATE tmerge SET state = zc.state
    FROM tmerge
    JOIN zipcodes zc
    ON LEFT(tmerge.zip,5) = zc.zipcode''', read='tsql')

# COMMAND ----------

sqlglot.parse_one('''MERGE INTO tmerge
USING zipcodes
ON LEFT(tmerge.zip, 5) = zipcodes.zipcode
WHEN MATCHED THEN
    UPDATE SET tmerge.state = zipcodes.state''', read='spark')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO tmerge
# MAGIC USING zipcodes
# MAGIC ON LEFT(tmerge.zip, 5) = zipcodes.zipcode
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET tmerge.state = zipcodes.state

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tmerge 

# COMMAND ----------

# MAGIC %md ### UPDATE FROM multiple joins
# MAGIC

# COMMAND ----------

tsql = """UPDATE ru_new  SET NN_officeID = id_nnid.identifier
      FROM t_reporting_user_merge ru_new
      JOIN CMM_repl..T_UserGroupMembership ugm
        ON ru_new.user_id = ugm.user_id
       AND ugm.role_id = 3
      JOIN CMM_repl..T_UserGroup ug
        ON ugm.group_id = ug.id
      JOIN CMM_repl..mtm_UserGroupIdentifier mtm_ugi_NNID
        ON ug.id = mtm_ugi_NNID.usergroup_id
      JOIN CMM_repl..T_Identifier id_NNID
        ON mtm_ugi_NNID.identifier_id = id_NNID.id
      JOIN CMM_repl..T_IdentifierType id_type_NNID
        ON id_type_NNID.identifier_type = id_NNID.identifier_type
       AND id_type_NNID.short_description like 'NNOfficeNID'
"""
sqlglot.transpile(tsql, read='tsql', write='databricks')[0]

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ru_new 
# MAGIC USING CMM_repl.T_UserGroupMembership AS ugm 
# MAGIC ON ru_new.user_id = ugm.user_id AND ugm.role_id = 3 
# MAGIC WHEN MATCHED THEN UPDATE SET NN_officeID = id_nnid.identifier

# COMMAND ----------

# MAGIC %sql
# MAGIC  UPDATE ru_new SET NPI = i.identifier
# MAGIC       FROM t_reporting_user_merge ru_new
# MAGIC       JOIN CMM_Repl..mtm_userIdentifier ui
# MAGIC         ON ru_new.user_id = ui.user_id
# MAGIC       JOIN CMM_Repl..t_identifier i
# MAGIC         ON ui.identifier_id = i.id
# MAGIC       JOIN CMM_Repl..t_identifiertype it
# MAGIC         ON i.identifier_type = it.identifier_type
# MAGIC      WHERE it.short_description = 'NPI'
# MAGIC         and i.identifier like replicate('[0-9]', 10)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- tsql
# MAGIC UPDATE ru_new SET NPI = i.identifier
# MAGIC       FROM t_merge ru_new
# MAGIC       JOIN mytbl1 ui
# MAGIC         ON ru_new.user_id = ui.user_id
# MAGIC       JOIN mytbl2 i
# MAGIC         ON ui.identifier_id = i.id
# MAGIC       JOIN mytble3 it
# MAGIC         ON i.identifier_type = it.identifier_type
# MAGIC      WHERE it.short_description = 'NPI'
# MAGIC         and i.identifier like replicate('[0-9]', 10)
# MAGIC
# MAGIC -- spark delta sql/databricks sql
# MAGIC MERGE INTO t_merge AS ru_new
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         ui.user_id,
# MAGIC         i.identifier AS npi
# MAGIC     FROM mytbl1 ui
# MAGIC     JOIN mytbl2 i
# MAGIC         ON ui.identifier_id = i.id
# MAGIC     JOIN mytble3 it
# MAGIC         ON i.identifier_type = it.identifier_type
# MAGIC     WHERE it.short_description = 'NPI'
# MAGIC         AND i.identifier rLIKE '^[0-9]{10}$'
# MAGIC ) AS src
# MAGIC ON ru_new.user_id = src.user_id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET ru_new.NPI = src.npi;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --- spark-delta SQL
# MAGIC use catalog douglas_moore;
# MAGIC use schema dbo;
# MAGIC create or replace table t_merge (user_id bigint, NPI CHAR(10));
# MAGIC create or replace table mytbl1 (user_id bigint, identifier_id bigint, NPI CHAR(10)); -- ui
# MAGIC create or replace table mytbl2 (id bigint, identifier string, identifier_type CHAR(10)); -- i
# MAGIC create or replace table mytbl3 (short_description CHAR(100), identifier_type CHAR(10)); -- it
# MAGIC
# MAGIC MERGE INTO t_merge AS ru_new
# MAGIC USING (SELECT ui.user_id, i.identifier 
# MAGIC FROM mytbl1 AS ui 
# MAGIC JOIN mytbl2 AS i ON ui.identifier_id = i.id 
# MAGIC JOIN mytbl3 AS it ON i.identifier_type = it.identifier_type 
# MAGIC
# MAGIC WHERE it.short_description = 'NPI' AND i.identifier RLIKE '^[0-9]{10}$') AS src
# MAGIC ON ru_new.user_id = src.user_id 
# MAGIC WHEN MATCHED THEN UPDATE SET ru_new.NPI = src.identifier

# COMMAND ----------

# MAGIC %md ## Unique constraint

# COMMAND ----------

import sqlglot

# COMMAND ----------

tsql = """CREATE TABLE [dbo].[t_external_cmm_email](
	[id] [int] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[email] [varchar](255) NOT NULL,
	[date_added] [datetime] NULL,
 CONSTRAINT [PK_t_external_cmm_email] PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [UN_t_external_cmm_email] UNIQUE NONCLUSTERED 
(
	[email] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]"""
sqls = sqlglot.transpile(tsql,read='tsql',write='databricks')
sqls[0]

# COMMAND ----------

sqlglot.parse_one(tsql, read='tsql')

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG douglas_moore;
# MAGIC CREATE TABLE `dbo`.`t_external_cmm_email` (
# MAGIC   `email` VARCHAR(255) NOT NULL, `date_added` TIMESTAMP,
# MAGIC   ,CONSTRAINT `UN_t_external_cmm_email` UNIQUE (`email`)
# MAGIC )

# COMMAND ----------

# MAGIC %md ## MERGE
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from douglas_moore.sqlglot.project1
# MAGIC where statement_type = 'MERGE'

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO common.t_reporting_user AS T 
# MAGIC USING common.t_reporting_user_merge AS S ON (T.user_id = S.user_id) 
# MAGIC WHEN NOT MATCHED THEN INSERT (user_id, username, display_name, organization_name, user_type_id, user_type, is_internal, is_navinet, can_create_realpa, is_verifiedNPI, address1, city, state, zip, email, phone, fax, office_contact, created_on, deleted_on, eula_agree, referral_source, referral_type, software_vendor, account_id, account_type_id, account_name, account_manager_user_id, group_name, user_category, user_bucket, npi, ncpdp, first_touch, last_touch, requests, real_Requests, paplus_requests, real_requests_per_day, paplus_requests_per_day, nn_officeid, user_branding_id, brand_name, eula_accepted, last_eula_version_accepted, last_eula_accepted_date) VALUES (s.user_id, s.username, s.display_name, s.organization_name, s.user_type_id, s.user_type, s.is_internal, s.is_navinet, s.can_create_realpa, s.is_verifiedNPI, s.address1, s.city, s.state, s.zip, s.email, s.phone, s.fax, s.office_contact, s.created_on, s.deleted_on, s.eula_agree, s.referral_source, s.referral_type, s.software_vendor, s.account_id, s.account_type_id, s.account_name, s.account_manager_user_id, s.group_name, user_category, s.user_bucket, s.npi, s.ncpdp, s.first_touch, s.last_touch, s.requests, s.real_Requests, s.paplus_requests, s.real_requests_per_day, s.paplus_requests_per_day, s.nn_officeid, s.user_branding_id, s.brand_name, s.eula_accepted, s.last_eula_version_accepted, s.last_eula_accepted_date) 
# MAGIC
# MAGIC WHEN MATCHED AND (COALESCE(T.username, '') <> COALESCE(S.username, '') OR COALESCE(T.display_name, '') <> COALESCE(S.display_name, '') OR COALESCE(T.organization_name, '') <> COALESCE(S.organization_name, '') OR COALESCE(T.user_type_id, '') <> COALESCE(S.user_type_id, '') OR COALESCE(T.user_type, '') <> COALESCE(S.user_type, '') OR COALESCE(T.is_internal, '') <> COALESCE(S.is_internal, '') OR COALESCE(T.is_navinet, '') <> COALESCE(S.is_navinet, '') OR COALESCE(T.can_create_realpa, '') <> COALESCE(S.can_create_realpa, '') OR COALESCE(T.is_verifiedNPI, '') <> COALESCE(S.is_verifiedNPI, '') OR COALESCE(T.address1, '') <> COALESCE(S.address1, '') OR COALESCE(T.city, '') <> COALESCE(S.city, '') OR COALESCE(T.state, '') <> COALESCE(S.state, '') OR COALESCE(T.zip, '') <> COALESCE(S.zip, '') OR COALESCE(T.email, '') <> COALESCE(S.email, '') OR COALESCE(T.phone, '') <> COALESCE(S.phone, '') OR COALESCE(T.fax, '') <> COALESCE(S.fax, '') OR COALESCE(T.office_contact, '') <> COALESCE(S.office_contact, '') OR COALESCE(T.created_on, '') <> COALESCE(S.created_on, '') OR COALESCE(T.deleted_on, '') <> COALESCE(S.deleted_on, '') OR COALESCE(T.eula_agree, '') <> COALESCE(S.eula_agree, '') OR COALESCE(T.referral_source, '') <> COALESCE(S.referral_source, '') OR COALESCE(T.referral_type, '') <> COALESCE(S.referral_type, '') OR COALESCE(T.software_vendor, '') <> COALESCE(S.software_vendor, '') OR COALESCE(T.account_id, '') <> COALESCE(S.account_id, '') OR COALESCE(T.account_type_id, '') <> COALESCE(S.account_type_id, '') OR COALESCE(T.account_name, '') <> COALESCE(S.account_name, '') OR COALESCE(T.account_manager_user_id, '') <> COALESCE(S.account_manager_user_id, '') OR COALESCE(T.group_name, '') <> COALESCE(S.group_name, '') OR COALESCE(T.user_category, '') <> COALESCE(S.user_category, '') OR COALESCE(T.user_bucket, '') <> COALESCE(S.user_bucket, '') OR COALESCE(T.npi, '') <> COALESCE(S.npi, '') OR COALESCE(T.ncpdp, '') <> COALESCE(S.ncpdp, '') OR COALESCE(T.first_touch, '') <> COALESCE(S.first_touch, '') OR COALESCE(T.last_touch, '') <> COALESCE(S.last_touch, '') OR COALESCE(T.requests, '') <> COALESCE(S.requests, '') OR COALESCE(T.real_Requests, '') <> COALESCE(S.real_Requests, '') OR COALESCE(T.paplus_requests, '') <> COALESCE(S.paplus_requests, '') OR COALESCE(T.real_requests_per_day, '') <> COALESCE(S.real_requests_per_day, '') OR COALESCE(T.paplus_requests_per_day, '') <> COALESCE(S.paplus_requests_per_day, '') OR COALESCE(T.nn_officeid, '') <> COALESCE(S.nn_officeid, '') OR COALESCE(T.user_branding_id, '') <> COALESCE(S.user_branding_id, '') OR COALESCE(T.brand_name, '') <> COALESCE(S.brand_name, '') OR COALESCE(t.eula_accepted, '') <> COALESCE(s.eula_accepted, '') OR COALESCE(t.last_eula_version_accepted, '') <> COALESCE(s.last_eula_version_accepted, '') OR COALESCE(t.last_eula_accepted_date, '') <> COALESCE(s.last_eula_accepted_date, '')) THEN UPDATE 
# MAGIC
# MAGIC SET T.username = S.username, T.display_name = S.display_name, T.organization_name = S.organization_name, T.user_type_id = S.user_type_id, T.user_type = S.user_type, T.is_internal = S.is_internal, T.is_navinet = S.is_navinet, T.can_create_realpa = S.can_create_realpa, T.is_verifiedNPI = S.is_verifiedNPI, T.address1 = S.address1, T.city = S.city, T.state = S.state, T.zip = S.zip, T.email = S.email, T.phone = S.phone, T.fax = S.fax, T.office_contact = S.office_contact, T.created_on = S.created_on, T.deleted_on = S.deleted_on, T.eula_agree = S.eula_agree, T.referral_source = S.referral_source, T.referral_type = S.referral_type, T.software_vendor = S.software_vendor, T.account_id = S.account_id, T.account_type_id = S.account_type_id, T.account_name = S.account_name, T.account_manager_user_id = S.account_manager_user_id, T.group_name = S.group_name, T.user_category = S.user_category, T.user_bucket = S.user_bucket, T.npi = S.npi, T.ncpdp = S.ncpdp, T.first_touch = S.first_touch, T.last_touch = S.last_touch, T.requests = S.requests, T.real_Requests = S.real_Requests, T.paplus_requests = S.paplus_requests, T.real_requests_per_day = S.real_requests_per_day, T.paplus_requests_per_day = S.paplus_requests_per_day, T.nn_officeid = S.nn_officeid, T.user_branding_id = S.user_branding_id, T.brand_name = S.brand_name, t.eula_accepted = s.eula_accepted, t.last_eula_version_accepted = s.last_eula_version_accepted, t.last_eula_accepted_date = s.last_eula_accepted_date

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO common.t_reporting_user AS T 
# MAGIC USING common.t_reporting_user_merge AS S ON (T.user_id = S.user_id) 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (user_id, username, display_name, organization_name, user_type_id, user_type, is_internal, is_navinet, can_create_realpa, is_verifiedNPI, address1, city, state, zip, email, phone, fax, office_contact, created_on, deleted_on, eula_agree, referral_source, referral_type, software_vendor, account_id, account_type_id, account_name, account_manager_user_id, group_name, user_category, user_bucket, npi, ncpdp, first_touch, last_touch, requests, real_Requests, paplus_requests, real_requests_per_day, paplus_requests_per_day, nn_officeid, user_branding_id, brand_name, eula_accepted, last_eula_version_accepted, last_eula_accepted_date) 
# MAGIC   VALUES (s.user_id, s.username, s.display_name, s.organization_name, s.user_type_id, s.user_type, s.is_internal, s.is_navinet, s.can_create_realpa, s.is_verifiedNPI, s.address1, s.city, s.state, s.zip, s.email, s.phone, s.fax, s.office_contact, s.created_on, s.deleted_on, s.eula_agree, s.referral_source, s.referral_type, s.software_vendor, s.account_id, s.account_type_id, s.account_name, s.account_manager_user_id, s.group_name, s.user_category, s.user_bucket, s.npi, s.ncpdp, s.first_touch, s.last_touch, s.requests, s.real_Requests, s.paplus_requests, s.real_requests_per_day, s.paplus_requests_per_day, s.nn_officeid, s.user_branding_id, s.brand_name, s.eula_accepted, s.last_eula_version_accepted, s.last_eula_accepted_date) 
# MAGIC WHEN MATCHED AND (
# MAGIC   COALESCE(T.username, '') <> COALESCE(S.username, '') 
# MAGIC   OR COALESCE(T.display_name, '') <> COALESCE(S.display_name, '') 
# MAGIC   OR COALESCE(T.organization_name, '') <> COALESCE(S.organization_name, '') 
# MAGIC   OR COALESCE(T.user_type_id, '') <> COALESCE(S.user_type_id, '') 
# MAGIC   OR COALESCE(T.user_type, '') <> COALESCE(S.user_type, '') 
# MAGIC   OR COALESCE(T.is_internal, '') <> COALESCE(S.is_internal, '') 
# MAGIC   OR COALESCE(T.is_navinet, '') <> COALESCE(S.is_navinet, '') 
# MAGIC   OR COALESCE(T.can_create_realpa, '') <> COALESCE(S.can_create_realpa, '') 
# MAGIC   OR COALESCE(T.is_verifiedNPI, '') <> COALESCE(S.is_verifiedNPI, '') 
# MAGIC   OR COALESCE(T.address1, '') <> COALESCE(S.address1, '') 
# MAGIC   OR COALESCE(T.city, '') <> COALESCE(S.city, '') 
# MAGIC   OR COALESCE(T.state, '') <> COALESCE(S.state, '') 
# MAGIC   OR COALESCE(T.zip, '') <> COALESCE(S.zip, '') 
# MAGIC   OR COALESCE(T.email, '') <> COALESCE(S.email, '') 
# MAGIC   OR COALESCE(T.phone, '') <> COALESCE(S.phone, '') 
# MAGIC   OR COALESCE(T.fax, '') <> COALESCE(S.fax, '') 
# MAGIC   OR COALESCE(T.office_contact, '') <> COALESCE(S.office_contact, '') 
# MAGIC   OR COALESCE(T.created_on, '') <> COALESCE(S.created_on, '') 
# MAGIC   OR COALESCE(T.deleted_on, '') <> COALESCE(S.deleted_on, '') 
# MAGIC   OR COALESCE(T.eula_agree, '') <> COALESCE(S.eula_agree, '') 
# MAGIC   OR COALESCE(T.referral_source, '') <> COALESCE(S.referral_source, '') 
# MAGIC   OR COALESCE(T.referral_type, '') <> COALESCE(S.referral_type, '') 
# MAGIC   OR COALESCE(T.software_vendor, '') <> COALESCE(S.software_vendor, '') 
# MAGIC   OR COALESCE(T.account_id, '') <> COALESCE(S.account_id, '') 
# MAGIC   OR COALESCE(T.account_type_id, '') <> COALESCE(S.account_type_id, '') 
# MAGIC   OR COALESCE(T.account_name, '') <> COALESCE(S.account_name, '') 
# MAGIC   OR COALESCE(T.account_manager_user_id, '') <> COALESCE(S.account_manager_user_id, '') 
# MAGIC   OR COALESCE(T.group_name, '') <> COALESCE(S.group_name, '') 
# MAGIC   OR COALESCE(T.user_category, '') <> COALESCE(S.user_category, '') 
# MAGIC   OR COALESCE(T.user_bucket, '') <> COALESCE(S.user_bucket, '') 
# MAGIC   OR COALESCE(T.npi, '') <> COALESCE(S.npi, '') 
# MAGIC   OR COALESCE(T.ncpdp, '') <> COALESCE(S.ncpdp, '') 
# MAGIC   OR COALESCE(T.first_touch, '') <> COALESCE(S.first_touch, '') 
# MAGIC   OR COALESCE(T.last_touch, '') <> COALESCE(S.last_touch, '') 
# MAGIC   OR COALESCE(T.requests, '') <> COALESCE(S.requests, '') 
# MAGIC   OR COALESCE(T.real_Requests, '') <> COALESCE(S.real_Requests, '') 
# MAGIC   OR COALESCE(T.paplus_requests, '') <> COALESCE(S.paplus_requests, '') 
# MAGIC   OR COALESCE(T.real_requests_per_day, '') <> COALESCE(S.real_requests_per_day, '') 
# MAGIC   OR COALESCE(T.paplus_requests_per_day, '') <> COALESCE(S.paplus_requests_per_day, '') 
# MAGIC   OR COALESCE(T.nn_officeid, '') <> COALESCE(S.nn_officeid, '') 
# MAGIC   OR COALESCE(T.user_branding_id, '') <> COALESCE(S.user_branding_id, '') 
# MAGIC   OR COALESCE(T.brand_name, '') <> COALESCE(S.brand_name, '') 
# MAGIC   OR COALESCE(t.eula_accepted, '') <> COALESCE(s.eula_accepted, '') 
# MAGIC   OR COALESCE(t.last_eula_version_accepted, '') <> COALESCE(s.last_eula_version_accepted, '') 
# MAGIC   OR COALESCE(t.last_eula_accepted_date, '') <> COALESCE(s.last_eula_accepted_date, '')
# MAGIC )
# MAGIC THEN 
# MAGIC   UPDATE SET 
# MAGIC     T.username = S.username, 
# MAGIC     T.display_name = S.display_name, 
# MAGIC     T.organization_name = S.organization_name, 
# MAGIC     T.user_type_id = S.user_type_id, 
# MAGIC     T.user_type = S.user_type, 
# MAGIC     T.is_internal = S.is_internal, 
# MAGIC     T.is_navinet = S.is_navinet, 
# MAGIC     T.can_create_realpa = S.can_create_realpa, 
# MAGIC     T.is_verifiedNPI = S.is_verifiedNPI, 
# MAGIC     T.address1 = S.address1, 
# MAGIC     T.city = S.city, 
# MAGIC     T.state = S.state, 
# MAGIC     T.zip = S.zip, 
# MAGIC     T.email = S.email, 
# MAGIC     T.phone = S.phone, 
# MAGIC     T.fax = S.fax, 
# MAGIC     T.office_contact = S.office_contact, 
# MAGIC     T.created_on = S.created_on, 
# MAGIC     T.deleted_on = S.deleted_on, 
# MAGIC     T.eula_agree = S.eula_agree, 
# MAGIC     T.referral_source = S.referral_source, 
# MAGIC     T.referral_type = S.referral_type, 
# MAGIC     T.software_vendor = S.software_vendor, 
# MAGIC     T.account_id = S.account_id, 
# MAGIC     T.account_type_id = S.account_type_id, 
# MAGIC     T.account_name = S.account_name, 
# MAGIC     T.account_manager_user_id = S.account_manager_user_id, 
# MAGIC     T.group_name = S.group_name, 
# MAGIC     T.user_category = S.user_category, 
# MAGIC     T.user_bucket = S.user_bucket, 
# MAGIC     T.npi = S.npi, 
# MAGIC     T.ncpdp = S.ncpdp, 
# MAGIC     T.first_touch = S.first_touch, 
# MAGIC     T.last_touch = S.last_touch, 
# MAGIC     T.requests = S.requests, 
# MAGIC     T.real_Requests = S.real_Requests, 
# MAGIC     T.paplus_requests = S.paplus_requests
# MAGIC

# COMMAND ----------

# MAGIC %md ## DECLARE VARIABLE
# MAGIC ```sql
# MAGIC DECLARE [ OR REPLACE ] [ VARIABLE ] variable_name
# MAGIC     [ data_type ] [ { DEFEAULT | = } default_expression ]
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from douglas_moore.sqlglot.project1
# MAGIC where statement_type = 'DECLARE'
# MAGIC --and context is null

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE VARIABLE bobby INT = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC DECLARE OR REPLACE VARIABLE MaxStartDate BIGINT ;
# MAGIC DECLARE OR REPLACE VARIABLE MaxTouchID BIGINT ;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET VAR `MaxTouchID` = (SELECT max(id) from range(10)	);

# COMMAND ----------

# MAGIC %sql
# MAGIC values(MaxTouchID)

# COMMAND ----------


