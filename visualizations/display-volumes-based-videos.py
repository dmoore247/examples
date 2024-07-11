# Databricks notebook source
# MAGIC %sh ls -lh /Volumes/douglas_moore/demo/visuals

# COMMAND ----------

ext_folder = "https://e2-demo-field-eng.cloud.databricks.com/ajax-api/2.0/fs/files/Volumes/douglas_moore/demo/visuals"
html = F'''
<html>
<head>
<style>
  .welcome_vid {{
    background: maroon;
    height: 1000px;
    display: block;
    margin-left: auto;
    margin-right: auto;
    width: 100%;
    border-radius: 10px;
    padding: 10px;
    box-shadow: 10px 10px 10px;
  }}
</style>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body>
  <video class="welcome_vid" controls autoplay loop>
    <source src="{ext_folder}/SampleVideo_1280x720_30mb.mp4" type="video/mp4">
    This browser does not display the video tag.
  </video>
  </body>
</html>
'''
displayHTML(html)
