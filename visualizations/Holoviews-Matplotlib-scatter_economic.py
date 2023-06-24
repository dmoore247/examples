# Databricks notebook source
# MAGIC %md 
# MAGIC # Holoviews with Matplotlib backend
# MAGIC ![](http://holoviews.org/_static/logo_horizontal.png)
# MAGIC
# MAGIC Download original IPython notebook from: http://holoviews.org/gallery/demos/matplotlib/scatter_economic.html
# MAGIC
# MAGIC ## Requirements
# MAGIC * `holoviews`
# MAGIC * `pandas`
# MAGIC * DBR ML Runtime (tested with DBR 6.2 beta ML)

# COMMAND ----------

import pandas as pd
import holoviews as hv
from holoviews import dim, opts

hv.extension('matplotlib')
hv.output(dpi=100)

# COMMAND ----------

# MAGIC %md ## Declaring data

# COMMAND ----------

macro_df = pd.read_csv('http://assets.holoviews.org/macro.csv', '\t')
key_dimensions   = [('year', 'Year'), ('country', 'Country')]
value_dimensions = [('unem', 'Unemployment'), ('capmob', 'Capital Mobility'),
                    ('gdp', 'GDP Growth'), ('trade', 'Trade')]
macro = hv.Table(macro_df, key_dimensions, value_dimensions)

# COMMAND ----------

# MAGIC %md ## Plot

# COMMAND ----------

gdp_unem_scatter = macro.to.scatter('Year', ['GDP Growth', 'Unemployment'])

gdp_unem_scatter.overlay('Country').opts(
    opts.Scatter(color=hv.Cycle('tab20'), edgecolors='k', show_grid=True,
                 aspect=2, fig_size=250, s=dim('Unemployment')*20,
                 padding=0.1, show_frame=False),
    opts.NdOverlay(legend_position='right'))

