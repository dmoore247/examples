# Databricks notebook source
# MAGIC %md
# MAGIC ### Monte Carlo Simulation with Python
# MAGIC 
# MAGIC Notebook to accompany article on [Practical Business Python](https://pbpython.com/monte-carlo.html)
# MAGIC 
# MAGIC Update to use numpy for faster loops based on comments [here](https://www.reddit.com/r/Python/comments/arxwkm/monte_carlo_simulation_with_python/)

# COMMAND ----------

import pandas as pd
import numpy as np
import seaborn as sns

# COMMAND ----------

sns.set_style('whitegrid')

# COMMAND ----------

# Define the variables for the Percent to target based on historical results
avg = 1
std_dev = .1
num_reps = 500
num_simulations = 100000

# COMMAND ----------

# Show an example of calculating the percent to target
pct_to_target = np.random.normal(
    avg,
    std_dev,
    size=(num_reps, num_simulations)
)

# COMMAND ----------

pct_to_target[0:10]

# COMMAND ----------

# Another example for the sales target distribution
sales_target_values = [75_000, 100_000, 200_000, 300_000, 400_000, 500_000]
sales_target_prob = [.3, .3, .2, .1, .05, .05]
sales_target = np.random.choice(sales_target_values, p=sales_target_prob, 
                                size=(num_reps, num_simulations))

# COMMAND ----------

sales_target[0:10]

# COMMAND ----------

commission_percentages = np.take(
    np.array([0.02, 0.03, 0.04]),
    np.digitize(pct_to_target, bins=[.9, .99, 10])
)

# COMMAND ----------

commission_percentages[0:10]

# COMMAND ----------

commission_percentages[:5]

# COMMAND ----------

total_commissions = (commission_percentages * sales_target).sum(axis=0)

# COMMAND ----------

total_commissions.std()

# COMMAND ----------

# Show how to create the dataframe
df = pd.DataFrame(data={'Total_Commissions': total_commissions})
df.head()

# COMMAND ----------

df.plot(kind='hist', title='Commissions Distribution')

# COMMAND ----------

df.describe()

# COMMAND ----------

df.columns

# COMMAND ----------

dfx.plot(kind='scatter', title='Commissions Scatter',x='x',y='y')

# COMMAND ----------


