# Databricks notebook source
# MAGIC %md # Geoviews
# MAGIC For displaying geographical information
# MAGIC From http://geoviews.org/index.html#
# MAGIC
# MAGIC This notebook demonstrates porting geoviews to Databricks. See companion notebook "Geoviews - Install"

# COMMAND ----------

import geoviews as gv
import geoviews.feature as gf
import xarray as xr
from cartopy import crs
import holoviews

gv.extension('matplotlib')
gv.output(size=300)

# COMMAND ----------

(gf.ocean + gf.land + gf.ocean * gf.land * gf.coastline * gf.borders).opts(
    'Feature', projection=crs.Geostationary(), global_extent=True).cols(3)

# COMMAND ----------

dataset = gv.Dataset(xr.open_dataset('./geoviews-examples/data/ensembles.nc'))
ensemble = dataset.to(gv.Image, ['longitude', 'latitude'], 'surface_temperature')

# COMMAND ----------

gv.output

# COMMAND ----------

from io import BytesIO
tmpfile = BytesIO()

gv.save(ensemble.opts(cmap='viridis', colorbar=True, fig_size=200, backend='matplotlib') * gf.coastline(), tmpfile)
html=tmpfile.getvalue().decode("UTF-8")
displayHTML(html)

# COMMAND ----------

pre_industrial = xr.open_dataset('./geoviews-examples/data/pre-industrial.nc').load()
air_temperature = gv.Dataset(pre_industrial, ['longitude', 'latitude'], 'air_temperature')
gv.Image(air_temperature)

# COMMAND ----------

air_temperature.to(gv.Image, ['longitude', 'latitude'])
