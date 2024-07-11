# Databricks notebook source
# MAGIC %sh 
# MAGIC ls -lh /Volumes/douglas_moore/demo/demo_videos

# COMMAND ----------

# MAGIC %sh cat /Volumes/douglas_moore/demo/demo_videos/review.html

# COMMAND ----------

html_file = "https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/demo_videos/volumes_review.html"
displayHTML(html_file)
