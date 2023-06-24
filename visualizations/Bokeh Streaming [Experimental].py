# Databricks notebook source
# MAGIC %sh /databricks/conda/bin/pip install IPython==7.14.0

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import IPython
print(IPython.__version__)

# COMMAND ----------

dbutils.library.installPyPI("bokeh")

# COMMAND ----------

# MAGIC %sh /databricks/conda/bin/pip freeze

# COMMAND ----------

import time
import numpy as np
# ------------------- new cell ---------------------#

from bokeh.models.sources import ColumnDataSource
from bokeh.plotting import figure
from bokeh.io import output_notebook, show, push_notebook
# ------------------- new cell ---------------------#

output_notebook()
# ------------------- new cell ---------------------#

my_figure = figure(plot_width=800, plot_height=400)
test_data = ColumnDataSource(data=dict(x=[0], y=[0]))
line = my_figure.line("x", "y", source=test_data)
handle = show(my_figure, notebook_handle=True)

# COMMAND ----------

print(handle)

# COMMAND ----------

help(show)

# COMMAND ----------

from IPython import get_ipython
ip = get_ipython()

# COMMAND ----------

print(getattr(ip, 'kernel', None))

# COMMAND ----------


if ip is None:
    return False
if not getattr(ip, 'kernel', None):
    return False
# No further checks are feasible
return True

# COMMAND ----------

print(handle.__dict__)

# COMMAND ----------



new_data=dict(x=[0], y=[0])
x = []
y = []

step = 0
step_size = 0.1  # increment for increasing step
max_step = 10  # arbitrary stop point for example
period = .1  # in seconds (simulate waiting for new data)
n_show = 10  # number of points to keep and show
while step < max_step:
    x.append(step)
    y.append(np.random.rand())
    new_data['x'] = x = x[-n_show:]  # prevent filling ram
    new_data['y'] = y = y[-n_show:]  # prevent filling ram

    test_data.stream(new_data, n_show)

    push_notebook(handle=handle)
    step += step_size
    time.sleep(period)

# COMMAND ----------


