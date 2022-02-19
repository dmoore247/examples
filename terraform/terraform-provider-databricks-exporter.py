# Databricks notebook source
# MAGIC %md
# MAGIC # My first Databricks Terraform exporter
# MAGIC - Exports entire environment my token has access to
# MAGIC 
# MAGIC Prerequsites:
# MAGIC - Databricks CLI to set the secret '--scope terraform, --key token'
# MAGIC - Small (e.g. m4) single node compute

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

# DBTITLE 1,Get Databricks credentials
import os

# set the secrets via databricks-cli
# example: databricks --profile E2DEMO secrets put --scope terraform --key e2-demo-field-eng

workspace = "e2-demo-field-eng"
os.environ["DATABRICKS_HOST"]=F"{workspace}.cloud.databricks.com" # 
os.environ["DATABRICKS_TOKEN"]=dbutils.secrets.get('terraform',F"{workspace}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the Databricks provder and 'connection'
# MAGIC 
# MAGIC - Could be split into multiple .tf files

# COMMAND ----------

# MAGIC %sh 
# MAGIC mkdir -p /tmp/terraform
# MAGIC cd /tmp/terraform

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd /tmp/terraform
# MAGIC cat <<EOF >databricks.tf
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

# MAGIC %sh 
# MAGIC cd /tmp/terraform
# MAGIC terraform init

# COMMAND ----------

# MAGIC %sh 
# MAGIC # convienience link
# MAGIC cd /tmp/terraform
# MAGIC ln -s .terraform/providers/registry.terraform.io/databrickslabs/databricks/0.5.0/linux_amd64/terraform-provider-databricks_v0.5.0 terraform-provider-databricks

# COMMAND ----------

# MAGIC %md ## Export workspace
# MAGIC Export $DATABRICKS_HOST workspace to *.tf files stored in local storage
# MAGIC 
# MAGIC Example:
# MAGIC ```
# MAGIC ./terraform-provider-databricks exporter -skip-interactive \
# MAGIC     -services=groups,secrets,access,compute,users,jobs,storage \
# MAGIC     -listing=jobs,compute \
# MAGIC     -last-active-days=90 \
# MAGIC     -debug
# MAGIC     ```

# COMMAND ----------

# DBTITLE 1,Run export
# MAGIC %sh 
# MAGIC cd /tmp/terraform
# MAGIC ./terraform-provider-databricks exporter -skip-interactive \
# MAGIC     -services=compute,jobs,storage \
# MAGIC     -listing=jobs,compute \
# MAGIC     -last-active-days=90 \
# MAGIC     -debug

# COMMAND ----------

# MAGIC %sh 
# MAGIC cd /tmp/terraform
# MAGIC tail tf.out

# COMMAND ----------


