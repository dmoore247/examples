# Databricks notebook source
# MAGIC %md
# MAGIC # My first Databricks Terraform script
# MAGIC - Creates a cluster
# MAGIC - Documents(https://docs.databricks.com/dev-tools/terraform/index.html)

# COMMAND ----------

# DBTITLE 1,Setup databricks api credentials file for the target workspace
import os
os.environ["DATABRICKS_HOST"]="demo.cloud.databricks.com" # CHANGEME
os.environ["DATABRICKS_TOKEN"]=dbutils.secrets.get('terraform','token') # set the secrets via databricks-cli

# COMMAND ----------

# MAGIC %md ## Install Terraform on Ubuntu node
# MAGIC 
# MAGIC Terraform Install Documents (https://learn.hashicorp.com/tutorials/terraform/install-cli)

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get update && sudo apt-get install -y gnupg software-properties-common curl
# MAGIC curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get update && sudo apt-get install terraform

# COMMAND ----------

# MAGIC %sh
# MAGIC terraform -help

# COMMAND ----------

# MAGIC %md
# MAGIC ## All in one Terraform script for Databricks workspace
# MAGIC 
# MAGIC - Could be split into multiple .tf files

# COMMAND ----------

# MAGIC %sh cat <<EOF >first-cluster.tf
# MAGIC #
# MAGIC # My first Databricks Terraform script
# MAGIC #
# MAGIC # Pre-requistes:
# MAGIC #   Get Databricks api token
# MAGIC #   pip install databricks-cli
# MAGIC #   databricks configure -token
# MAGIC #   brew install jq
# MAGIC #   install terraform  (https://learn.hashicorp.com/tutorials/terraform/install-cli)
# MAGIC #
# MAGIC # Run:
# MAGIC #  copy this to demo-shard.tf
# MAGIC #  terraform init
# MAGIC #  terraform plan
# MAGIC #  terraform apply 
# MAGIC #
# MAGIC 
# MAGIC terraform {
# MAGIC   required_providers {
# MAGIC     databricks = {
# MAGIC       source  = "databrickslabs/databricks"
# MAGIC       version = "0.4.9"
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC provider "databricks" {
# MAGIC   # defaults to looking at ~/.databrickscfg or env variables (not set in this example)
# MAGIC }
# MAGIC 
# MAGIC data "databricks_current_user" "me" {}
# MAGIC data "databricks_node_type" "smallest" {
# MAGIC   local_disk = true
# MAGIC }
# MAGIC 
# MAGIC data "databricks_spark_version" "latest_lts" {
# MAGIC   long_term_support = true
# MAGIC }
# MAGIC 
# MAGIC resource "databricks_cluster" "shared_autoscaling" {
# MAGIC   cluster_name            = "dm-terraform-test-2"
# MAGIC   spark_version           = data.databricks_spark_version.latest_lts.id
# MAGIC   node_type_id            = data.databricks_node_type.smallest.id
# MAGIC   autotermination_minutes = 23
# MAGIC   autoscale {
# MAGIC     min_workers = 1
# MAGIC     max_workers = 7
# MAGIC   }
# MAGIC }
# MAGIC EOF

# COMMAND ----------

# MAGIC %sh ls -lat *.tf

# COMMAND ----------

# MAGIC %md ## Initialize Terraform

# COMMAND ----------

# MAGIC %sh terraform init

# COMMAND ----------

# MAGIC %md ## Begin transforming your workspace

# COMMAND ----------

# DBTITLE 1,Compute and show the plan move from the current state to the end state described in first-cluster.tf
# MAGIC %sh terraform plan

# COMMAND ----------

# DBTITLE 1,Save the plan to a file
# MAGIC %sh terraform plan -out out1

# COMMAND ----------

# DBTITLE 1,Apply the plan to Databricks workspace
# MAGIC %sh
# MAGIC terraform apply -auto-approve

# COMMAND ----------

# MAGIC %sh terraform plan

# COMMAND ----------

# MAGIC %sh terraform show

# COMMAND ----------

# MAGIC %md ## Save off the Terraform state of your workspace

# COMMAND ----------

# todo
