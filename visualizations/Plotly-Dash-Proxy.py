# Databricks notebook source
# MAGIC %sh pip install dash==1.8.0

# COMMAND ----------

import dash
import dash_core_components as dcc
import dash_html_components as html

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': [1, 2, 3], 'y': [4, 1, 2], 'type': 'bar', 'name': 'SF'},
                {'x': [1, 2, 3], 'y': [2, 4, 5], 'type': 'bar', 'name': u'Montréal'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    )
])

if __name__ == '__main__':
  app.run_server(debug=False, host='0.0.0.0', port=9098)


# COMMAND ----------

displayHTML("""<a href='https://demo.cloud.databricks.com/driver-proxy-api/o/0/0120-203414-pried30/9098/?token=dapi174f72028038af1b455cb7fe45c90723'>Launch Dash</a> """)

# COMMAND ----------

# MAGIC %md ## Plotly Dash dashboard
# MAGIC * Share data between Dash dashboard developers, Notebook developers, production jobs, Data Science

# COMMAND ----------

# MAGIC %sql select * from delta.`/tmp/dmoore/gapminder`

# COMMAND ----------


