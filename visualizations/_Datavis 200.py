# Databricks notebook source
# MAGIC %md # Datavis 200
# MAGIC
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Audience: 
# MAGIC SA/Customers need to know once they've explored Databricks display() and matplotlib in passing
# MAGIC
# MAGIC ## Default Databricks rendering capabilities
# MAGIC * Spark Dataframes
# MAGIC * Matplotlib via `display()`
# MAGIC * HTML via `displayHTML()`
# MAGIC * `plotly`
# MAGIC
# MAGIC ## Expanding visualization capabilities with 3rd party packages
# MAGIC We can adopt these strategies to extend Databricks visualization strategies both in the notebook and headless use cases such as with MLFlow
# MAGIC 0. Save to PNG and embed into HTML
# MAGIC 0. Use Databricks display() and close the plot
# MAGIC 0. Render to HTML
# MAGIC 0. Inline Matplotlib
# MAGIC
# MAGIC Use these strategies with MLFlow to log plots
# MAGIC
# MAGIC We'll start with the Python stalwart Matplotlib

# COMMAND ----------


