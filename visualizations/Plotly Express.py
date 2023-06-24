# Databricks notebook source
# %pip install plotly

# COMMAND ----------

import plotly.express as px
df = px.data.iris()
fig = px.scatter_matrix(df)
fig.show()

# COMMAND ----------

df = spark.sql("select ....").toPandas()

# COMMAND ----------

#working in jupyter
import plotly.express as px
df = px.data.gapminder()
fig = px.scatter(df, x="gdpPercap", y="lifeExp", animation_frame="year", animation_group="country",
           size="pop", color="continent", hover_name="country",
           log_x=True, size_max=55, range_x=[100,100000], range_y=[25,90])
fig["layout"].pop("updatemenus") # optional, drop animation buttons
fig.show()
