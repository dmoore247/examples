-- Databricks notebook source
-- MAGIC %md # Curate Bronze Cluster information into Silver layer

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.widgets.text("schema_name", defaultValue="main.douglas_moore", label="Target Schema")
-- MAGIC
-- MAGIC schema_name = dbutils.widgets.get("schema_name")
-- MAGIC spark.conf.set('c.schema_name',schema_name)
-- MAGIC spark.sql(f'use {schema_name}')
-- MAGIC schema_name, spark.conf.get('c.schema_name')

-- COMMAND ----------

select * from bronze_cluster_events

-- COMMAND ----------

-- MAGIC %md # Standardize Silver Layer
-- MAGIC - Deduplicate what's in bronze

-- COMMAND ----------

-- De-duplicate bronze cluster events into silver cluster events. Format time.

CREATE TABLE IF NOT EXISTS silver_cluster_events (
  cluster_id STRING NOT NULL,
  time_est TIMESTAMP NOT NULL,
  type STRING NOT NULL COMMENT 'Cluster event type',
  details MAP < STRING, STRING >,
  load_dt TIMESTAMP NOT NULL,
  host STRING NOT NULL
) USING delta;

INSERT OVERWRITE silver_cluster_events
SELECT 
  cluster_id, 
  from_utc_timestamp(cast(timestamp/1000 as timestamp),'America/New_York') as time_est, 
  coalesce(type,regexp_extract_all(cast(details.reason as STRING), '.*code=(\\w+)')[0]) as type, 
  cast(details as MAP<STRING, STRING>) as details,
  load_dt,
  host
FROM (
  SELECT 
    *, 
    rank() 
      OVER (PARTITION BY cluster_id, timestamp, type, host
      ORDER BY cluster_id, timestamp, type, host, load_dt DESC) as rank
  FROM bronze_cluster_events
)
WHERE rank = 1

-- COMMAND ----------

select * from silver_cluster_events

-- COMMAND ----------

select 
  host,
  count(distinct cluster_id) distinct_clusters, 
  count(distinct cluster_id, time_est,type, cast(details as string)) distinct_events,
  count(1) event_count
from silver_cluster_events
group by host

-- COMMAND ----------

select count(1), cluster_id, time_est, type, cast(details as string), load_dt, host
from silver_cluster_events
group by cluster_id, time_est, type, cast(details as string), load_dt, host
having count(1) > 1

-- COMMAND ----------

-- MAGIC %md ## De-duplicate clusters info

-- COMMAND ----------

CREATE OR REPLACE TABLE silver_clusters as
SELECT
  cluster_id,
  autoscale,
  autotermination_minutes,
  aws_attributes,
  cluster_cores,
  cluster_memory_mb,
  cluster_name,
  cluster_source,
  creator_user_name,
  custom_tags,
  data_security_mode,
  default_tags,
  driver,
  driver_node_type_id,
  enable_elastic_disk,
  enable_local_disk_encryption,
  executors,
  init_scripts,
  jdbc_port,
  last_restarted_time,
  last_state_loss_time,
  node_type_id,
  runtime_engine,
  spark_conf,
  spark_context_id,
  spark_version,
  start_time,
  state,
  state_message,
  policy_id,
  cluster_log_conf,
  cluster_log_status,
  num_workers,
  spark_env_vars,
  single_user_name,
  terminated_time,
  termination_reason,
  driver_instance_pool_id,
  instance_pool_id,
  host,
  load_dt
FROM
  (
    SELECT
      cluster_id,
      autoscale,
      autotermination_minutes,
      aws_attributes,
      cluster_cores,
      cluster_memory_mb,
      cluster_name,
      cluster_source,
      creator_user_name,
      custom_tags,
      data_security_mode,
      default_tags,
      driver,
      driver_node_type_id,
      enable_elastic_disk,
      enable_local_disk_encryption,
      executors,
      init_scripts,
      jdbc_port,
      last_restarted_time,
      last_state_loss_time,
      node_type_id,
      runtime_engine,
      spark_conf,
      spark_context_id,
      spark_version,
      start_time,
      state,
      state_message,
      policy_id,
      cluster_log_conf,
      cluster_log_status,
      num_workers,
      spark_env_vars,
      single_user_name,
      terminated_time,
      termination_reason,
      driver_instance_pool_id,
      instance_pool_id,
      host,
      load_dt,
      ROW_NUMBER() OVER (
        PARTITION BY cluster_id,
        load_dt,
        host
        ORDER BY
          load_dt DESC
      ) as rnk
    FROM
      bronze_clusters
  ) tmp
WHERE
  rnk = 1;

-- COMMAND ----------

select * from silver_cluster_events order by cluster_id, time_est

-- COMMAND ----------

select 
  c.cluster_name,
  c.cluster_id, 
  e.time_est, 
  e.type, 
  e.details
from silver_cluster_events e
JOIN silver_clusters c ON e.cluster_id = c.cluster_id
order by 1,3

-- COMMAND ----------


