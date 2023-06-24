# Databricks notebook source
# MAGIC %sh conda install pandas=0.19.2

# COMMAND ----------

# MAGIC %sh
# MAGIC pip uninstall ggplot -y
# MAGIC pip install git+git://github.com/yhat/ggpy.git@9d00182343eccca6486beabd256e8c89fb0c59e8 --no-cache

# COMMAND ----------

from ggplot import *

( 
  ggplot(
    diamonds, 
    aes(x='carat', 
        y='price', 
        color='cut')
  ) + 
  geom_point() +
  scale_color_brewer(type='diverging', palette=4) +
  xlab("Carats") + 
  ylab("Price") + 
  ggtitle("Diamonds")
)


# COMMAND ----------


