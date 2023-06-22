-- Databricks notebook source
-- MAGIC %md # Create Gold Tables

-- COMMAND ----------

-- MAGIC %md ## Setup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("schema_name", defaultValue="main.douglas_moore", label="Target Schema")
-- MAGIC
-- MAGIC schema_name = dbutils.widgets.get("schema_name")
-- MAGIC spark.conf.set('c.schema_name',schema_name)
-- MAGIC spark.sql(f'use {schema_name}')
-- MAGIC schema_name, spark.conf.get('c.schema_name')

-- COMMAND ----------

select distinct type from bronze_cluster_events

-- COMMAND ----------

CREATE OR REPLACE table cluster_event_types (`type` STRING, `seq` BIGINT);

insert OVERWRITE cluster_event_types(`type`, `seq`) 
values 
  ('CREATING',0),
  ('STARTING',0),
  ('EDITED',0),
  ('RESTARTING',0),
  ('INIT_SCRIPTS_STARTED',2),
  ('INIT_SCRIPTS_FINISHED',3),
  ('RUNNING',4),
  ('DRIVER_HEALTHY',5),
  ('RESIZING',6),
  ('UPSIZE_COMPLETED',7),
  ('TERMINATING',9)
;

-- COMMAND ----------

-- MAGIC %md ## Create gold_cluster_event_analysis table

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_cluster_event_analysis AS
WITH cte1 AS (
   SELECT e.host, 
          e.cluster_id, 
          time_est, 
          e.type, 
          row_number() OVER(PARTITION BY e.cluster_id ORDER BY time_est) AS row_num
   FROM silver_cluster_events e
   LEFT OUTER JOIN main.douglas_moore.cluster_event_types c 
   ON e.type = c.type
), 
cte2 AS (
   SELECT host, 
          cluster_id, 
          type, 
          time_est, 
          row_num, 
          SUM(CASE WHEN (type = 'STARTING' OR type = 'CREATING' OR type = 'RESTARTING' or type = 'EDITED') THEN 1 ELSE 0 END) OVER(PARTITION BY cluster_id ORDER BY time_est) AS seq_num
   FROM cte1
), 
starting_times_cte AS (
   SELECT host, 
          cluster_id, 
          seq_num AS starting_seq_num, 
          MIN(time_est) AS starting_time_est,
          CONCAT(cluster_id, '_', seq_num) AS session_id
   FROM cte2
   WHERE (type = 'STARTING' OR type = 'CREATING' OR type = 'RESTARTING' or type = 'EDITED')
   GROUP BY host, cluster_id, seq_num
)

SELECT 
       s.session_id,
       sc.cluster_name, 
       c.cluster_id, 
       c.type,
       CAST((c.time_est - s.starting_time_est) AS INT) AS seconds_elapsed,
       c.time_est,
       CONCAT(
           CONCAT(FLOOR((CAST(c.time_est - s.starting_time_est AS INT))/3600), ':'),
           LPAD(FLOOR((MOD((CAST(c.time_est - s.starting_time_est AS INT)), 3600))/60), 2, '0'), ':',
           LPAD(FLOOR(MOD((CAST(c.time_est - s.starting_time_est AS INT)), 60)), 2, '0')
       ) AS elapsed_time_formatted,
       sc.driver_node_type_id, 
       sc.node_type_id, 
       sc.num_workers, 
       c.host
FROM cte2 AS c 
JOIN starting_times_cte AS s
ON c.cluster_id = s.cluster_id AND c.seq_num = s.starting_seq_num
LEFT JOIN (select distinct cluster_id, cluster_name, driver_node_type_id, node_type_id, num_workers from silver_clusters) sc 
ON c.cluster_id = sc.cluster_id
WHERE (type <> 'STARTING' OR type <> 'CREATING' OR type <> 'RESTARTING'  or type <> 'EDITED')
ORDER BY c.host, c.cluster_id ASC, c.time_est ASC

-- COMMAND ----------

select 
  count(distinct host) hosts,
  count(distinct cluster_id) clusters,
  count(distinct driver_node_type_id, node_type_id, num_workers) configurations,
  count(distinct session_id) sessions, 
  count(1) events 
from gold_cluster_event_analysis

-- COMMAND ----------

-- MAGIC %md ## Create gold_cluster_event_analysis_filtered
-- MAGIC Repeated events filtered to generate more accurate startup timing analysis

-- COMMAND ----------

CREATE OR REPLACE TABLE gold_cluster_event_analysis_filtered AS
WITH filtered_events AS (
SELECT
   session_id,
   row_number() OVER (PARTITION BY session_id,type ORDER BY time_est) seq,
   cluster_id, 
   cluster_name, 
   type, 
   seconds_elapsed, 
   time_est, 
   elapsed_time_formatted,
   driver_node_type_id,
   node_type_id,
   num_workers,
   host,
   row_number() OVER (PARTITION BY session_id,type ORDER BY time_est) rn

FROM gold_cluster_event_analysis gcea
)
select * except(rn) FROM filtered_events where rn = 1
ORDER BY session_id, time_est

-- COMMAND ----------

describe table gold_cluster_event_analysis_filtered

-- COMMAND ----------


