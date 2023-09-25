# Databricks notebook source
# MAGIC %md # Automatically load a policy
# MAGIC - Run this notebook as administrator

# COMMAND ----------

# MAGIC %pip install databricks-cli

# COMMAND ----------

instance_pool_name = "etl_pool-6"
policy_name = "jobs-pool-policy-generated-6"

# COMMAND ----------

# MAGIC %md ## Get an api connection

# COMMAND ----------

import json

from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import get_config
api_client = _get_api_client(get_config())

# COMMAND ----------

# MAGIC %md ## Create an instance pool

# COMMAND ----------

from databricks_cli.instance_pools.api import InstancePoolService
ips = InstancePoolService(api_client)
ips.list_instance_pools()

# COMMAND ----------

ips_response = ips.create_instance_pool(
    instance_pool_name=instance_pool_name,
    min_idle_instances = 0,
    max_capacity = 40,
    aws_attributes = {
      'availability': 'SPOT',
      'zone_id': 'us-west-2a',
      'spot_bid_price_percent': 100},
    node_type_id = 'c5d.2xlarge',
    idle_instance_autotermination_minutes = 15,
    enable_elastic_disk = False,
    disk_spec = {'disk_type': {'ebs_volume_type': 'GENERAL_PURPOSE_SSD'},
      'disk_count': 3,
      'disk_size': 100},
    preloaded_spark_versions = ['7.3.x-scala2.12'],
    custom_tags = {
      'Project': 'Infosec ETL',
      'Environment': 'Production',
      'Deparment': 'Sales'}
)

# COMMAND ----------

instance_pool_id = ips_response['instance_pool_id']

# COMMAND ----------

ips.get_instance_pool(instance_pool_id=instance_pool_id)

# COMMAND ----------

# MAGIC %md ## Create a cluster policy

# COMMAND ----------

from databricks_cli.cluster_policies.api import ClusterPolicyApi

# COMMAND ----------

policy = {  
  "cluster_source":   {"type": "fixed",  "value": "JOB" },
  "dbus_per_hour":    {"type": "range",   "maxValue": 100},
  "instance_pool_id": {"type": "fixed",   "value": F"{instance_pool_id}",    "hidden": False },
  "driver_instance_pool_id": {   "type": "fixed",   "value": F"{instance_pool_id}", "hidden": False },
  "num_workers":      {  "type": "range",  "maxValue": 20},
  "spark_version":    {  "type": "regex", "pattern": "7\\\\.[0-9]+\\\\.x-scala.*"  },
  "custom_tags.team": {  "type": "fixed",   "value": "product"  }
}

# COMMAND ----------

policy_wrapper = {
 'name': F'{policy_name}',
 'definition': F'{json.dumps(policy)}'
}

# COMMAND ----------

ClusterPolicyApi(api_client).create_cluster_policy(policy_wrapper)
