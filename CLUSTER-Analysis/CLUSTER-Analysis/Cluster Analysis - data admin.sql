-- Databricks notebook source
use hive_metastore.cto_ustst_alyt_eds

-- COMMAND ----------

CREATE OR REPLACE TABLE bronze_clusters (
  cluster_id STRING NOT NULL,
  cluster_name STRING,
  autoscale MAP<STRING, BIGINT>,
  autotermination_minutes BIGINT,
  aws_attributes MAP<STRING, STRING>,
  cluster_cores DOUBLE, 
  cluster_memory_mb BIGINT,
  cluster_source STRING,
  creator_user_name STRING,
  custom_tags MAP<STRING, STRING>,
  data_security_mode STRING,
  default_tags MAP<STRING, STRING>,
  driver MAP<STRING, STRING>,
  driver_node_type_id STRING,
  enable_elastic_disk BOOLEAN,
  enable_local_disk_encryption BOOLEAN,
  executors ARRAY<MAP<STRING, STRING>>,
  init_scripts ARRAY<MAP<STRING, MAP<STRING, STRING>>>,
  jdbc_port BIGINT,
  last_restarted_time BIGINT,
  last_state_loss_time BIGINT,
  node_type_id STRING,
  runtime_engine STRING,
  spark_conf MAP<STRING, STRING>,
  spark_context_id BIGINT,
  spark_version STRING,
  start_time BIGINT,
  state STRING,
  state_message STRING,
  policy_id STRING,
  cluster_log_conf MAP<STRING, MAP<STRING, STRING>>,
  cluster_log_status MAP<STRING, STRING>,
  num_workers BIGINT,
  spark_env_vars MAP<STRING, STRING>,
  single_user_name STRING,
  terminated_time BIGINT,
  termination_reason MAP<STRING, STRING>,
  driver_instance_pool_id STRING,
  instance_pool_id STRING,
  load_dt TIMESTAMP NOT NULL)
USING delta
PARTITIONED BY (host STRING)
TBLPROPERTIES (
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '2')

-- COMMAND ----------

insert into bronze_clusters2(cluster_id, cluster_name, autoscale, autotermination_minutes, aws_attributes, cluster_cores, cluster_memory_mb, cluster_source, creator_user_name, custom_tags, data_security_mode, default_tags, driver, driver_node_type_id, enable_elastic_disk, enable_local_disk_encryption, executors, init_scripts, jdbc_port, last_restarted_time, last_state_loss_time, node_type_id, runtime_engine, spark_conf, spark_context_id, spark_version, start_time, state, state_message, policy_id, cluster_log_conf, cluster_log_status, num_workers, spark_env_vars, single_user_name, terminated_time, termination_reason, driver_instance_pool_id, instance_pool_id, load_dt, host)

select cluster_id, cluster_name, autoscale, autotermination_minutes, aws_attributes, cluster_cores, cluster_memory_mb, cluster_source, creator_user_name, custom_tags, data_security_mode, default_tags, driver, driver_node_type_id, enable_elastic_disk, enable_local_disk_encryption, executors, init_scripts, jdbc_port, last_restarted_time, last_state_loss_time, node_type_id, runtime_engine, spark_conf, spark_context_id, spark_version, start_time, state, state_message, policy_id, cluster_log_conf, cluster_log_status, num_workers, spark_env_vars, single_user_name, terminated_time, termination_reason, driver_instance_pool_id, instance_pool_id, load_dt, host
from bronze_clusters

-- COMMAND ----------


