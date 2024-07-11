# Databricks notebook source
# MAGIC %pip install databricks_dash

# COMMAND ----------

# MAGIC %pip install plotly

# COMMAND ----------

import plotly.express as px

# Carregar dados de exemplo
df = px.data.tips()

# Criar o gráfico de dispersão
fig = px.scatter(df, x="total_bill", y="tip", color="size", title="Tips")

# Exibir o gráfico
fig.show()

# COMMAND ----------

# MAGIC %pip install verde

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import verde as vd
import numpy as np

# Criar uma grade regular de pontos
lons, lats = np.meshgrid(np.linspace(-180, 180, 100), np.linspace(-90, 90, 100))

# Gerar valores de exemplo para os pontos
values = np.sin(np.radians(lats)) * np.cos(np.radians(lons))

# Criar um objeto verde.Gridder para realizar a interpolação
gridder = vd.ScipyGridder(method="cubic")

# Realizar a interpolação
grid = gridder.fit((lons.flatten(), lats.flatten()), values.flatten())

# Obter os valores interpolados em uma nova grade
interp_lons, interp_lats = np.meshgrid(np.linspace(-180, 180, 200), np.linspace(-90, 90, 200))
interp_values = grid.predict((interp_lons.flatten(), interp_lats.flatten()))

# Plotar os resultados
import matplotlib.pyplot as plt
plt.contourf(interp_lons, interp_lats, interp_values.reshape(interp_lons.shape))
plt.colorbar(label="Valor")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Interpolação Espacial")
plt.show()

# COMMAND ----------

!pip install --upgrade verde

# COMMAND ----------

# Imports
import plotly.express as px
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from databricks_dash import DatabricksDash
# Load Data
df = px.data.tips()
# Build App
app = DatabricksDash(__name__)
server = app.server
app.layout = html.Div([
     html.H1("DatabricksDash Demo"),
     dcc.Graph(id='graph'),
     html.Label([
          "colorscale",
          dcc.Dropdown(
               id='colorscale-dropdown', clearable=False,
               value='plasma', options=[
                    {'label': c,'value': c}
                    for c in px.colors.named_colorscales()
               ])
     ]),
])
# Define callback to update graph
@app.callback(
     Output('graph', 'figure'),
     [Input("colorscale-dropdown", "value")]
)
def update_figure(colorscale):
     return px.scatter(
          df, x="total_bill", y="tip", color="size",
          color_continuous_scale=colorscale,
          render_mode="webgl", title="Tips"
     )
if __name__ == "__main__":
     app.run_server(mode='inline', debug=True)

# COMMAND ----------

# MAGIC %md # Try Dash

# COMMAND ----------

# MAGIC %pip install plotly dash

# COMMAND ----------

import plotly.io as pio
pio.renderers.default = "databricks"

# COMMAND ----------

# MAGIC %sh python -m flask
# MAGIC
# MAGIC export FLASK_APP=hello.py
# MAGIC export FLASK_ENV=development
# MAGIC flask run

# COMMAND ----------

# MAGIC %sh ls *.py

# COMMAND ----------

from flask import Flask
import dash

server = Flask(__name__)

# COMMAND ----------

import plotly.graph_objects as go # or plotly.express as px
fig = go.Figure(
    data=[go.Bar(y=[2, 1, 3])],
    layout_title_text="A Figure Displayed with the 'databricks' Renderer"
)

import dash
import dash_core_components as dcc
import dash_html_components as html

app = dash.Dash(title='my app')
app.layout = html.Div([
    dcc.Graph(figure=fig)
])

app.run_server(debug=True, use_reloader=False)  # Turn off reloader if inside Jupyter

# COMMAND ----------

# MAGIC %md # Plotly with Databricks renderer

# COMMAND ----------

import plotly.graph_objects as go
fig = go.Figure(
    data=[go.Bar(y=[2, 1, 3])],
    layout_title_text="A Figure Displayed with the 'databricks' Renderer"
)
fig.show(renderer="databricks")

# COMMAND ----------


