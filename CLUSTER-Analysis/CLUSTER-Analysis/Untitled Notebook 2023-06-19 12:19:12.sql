-- Databricks notebook source
use main.douglas_moore

-- COMMAND ----------

-- details['reason'] is a string field that contains code=INIT_SCRIPT_FAILURE, type=CLIENT_ERROR, parameters={databricks_error_message=Cluster scoped init script dbfs:/databricks/db_repos_proxy/db_repos_proxy_init.sh failed: Timed out with exception after 5 attempts (debugStr = 'Reading remote file for init script'), Caused by: java.io.FileNotFoundException: No such file or directory: s3a://shard-demo-dbfs-root/shard-demo/0/databricks/db_repos_proxy/db_repos_proxy_init.sh, instance_id=i-0875a0ef34092c084}}

-- extract the code field and return that as typex

select 
    coalesce(type,regexp_extract_all(cast(details.reason as STRING), '.*code=(\\w+)')[0]) as type
from bronze_cluster_events 
--where type is null
--limit 10

-- COMMAND ----------


