# Databricks notebook source
# MAGIC %md
# MAGIC # My first Databricks Terraform exporter
# MAGIC - Exports entire environment my token has access to
# MAGIC 
# MAGIC Prerequsites:
# MAGIC - Databricks CLI to set the secret '--scope terraform, --key token'
# MAGIC - Small (e.g. m4) single node compute

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
# MAGIC sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com $(lsb_release -cs) main"
# MAGIC sudo apt-get update && sudo apt-get install terraform
# MAGIC terraform -help

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Databricks provder and 'connection'
# MAGIC 
# MAGIC - Could be split into multiple .tf files

# COMMAND ----------

# MAGIC %sh cat <<EOF >databricks.tf
# MAGIC terraform {
# MAGIC   required_providers {
# MAGIC     databricks = {
# MAGIC       source  = "databrickslabs/databricks"
# MAGIC       version = "0.5.0"
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC 
# MAGIC provider "databricks" {
# MAGIC   # defaults to looking at ~/.databrickscfg or DATABRICKS_* env variables
# MAGIC }
# MAGIC EOF

# COMMAND ----------

# MAGIC %md ## Initialize Terraform

# COMMAND ----------

# MAGIC %sh terraform init

# COMMAND ----------

# MAGIC %sh ls -al

# COMMAND ----------

# MAGIC %sh find .terraform -print

# COMMAND ----------

# MAGIC %sh 
# MAGIC # convienience link
# MAGIC ln -s .terraform/providers/registry.terraform.io/databrickslabs/databricks/0.5.0/linux_amd64/terraform-provider-databricks_v0.5.0 terraform-provider-databricks

# COMMAND ----------

# MAGIC %md ## Export workspace
# MAGIC Export $DATABRICKS_HOST workspace to *.tf files stored in local storage

# COMMAND ----------

# DBTITLE 1,Run export
# MAGIC %sh nohup ./terraform-provider-databricks exporter -skip-interactive >tf.out &

# COMMAND ----------

# MAGIC %sh tail -f tf.out

# COMMAND ----------


