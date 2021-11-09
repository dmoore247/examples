# Databricks notebook source
# MAGIC %pip install jax jaxlib

# COMMAND ----------

pip freeze

# COMMAND ----------

import os
#os.environ['TF_CPP_MIN_LOG_LEVEL']
#os.unsetenv('TF_CPP_MIN_LOG_LEVEL')
os.environ['TF_CPP_MIN_LOG_LEVEL']

# COMMAND ----------

# MAGIC %sh export TF_CPP_MIN_LOG_LEVEL=0

# COMMAND ----------

import jax.numpy as jnp
from jax import jit

def slow_f(x):
  # Element-wise ops see a large benefit from fusion
  return x * x + x * 2.0

x = jnp.ones((5000, 5000))
fast_f = jit(slow_f)
%timeit -n10 -r3 fast_f(x)  # ~ 4.5 ms / loop on Titan X
%timeit -n10 -r3 slow_f(x)  # ~ 14.5 ms / loop (also on GPU via JAX)

# COMMAND ----------


