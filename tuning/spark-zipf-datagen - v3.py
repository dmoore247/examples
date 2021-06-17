# Databricks notebook source
# MAGIC %md # Joining Skewed data
# MAGIC 
# MAGIC Examples of tuning a skewed join. We uses the Zipf distribution to generate simulated skewed data sets for both sides of the join.
# MAGIC 
# MAGIC author: Douglas Moore
# MAGIC 
# MAGIC tags: optimization, tuning, skewed join, join hint, broadcast join, zipf, data generator
# MAGIC 
# MAGIC ### Requirements:
# MAGIC - DBR 8.2+
# MAGIC - Pandas
# MAGIC - Numpy
# MAGIC 
# MAGIC ### Resources:
# MAGIC 0. [Adaptive query execution](https://docs.databricks.com/spark/latest/spark-sql/aqe.html#adaptive-query-execution)
# MAGIC 0. [Join Hints](https://spark.apache.org/docs/3.0.0/sql-ref-syntax-qry-select-hints.html)
# MAGIC 0. [numpy.random.zipf](https://numpy.org/doc/stable/reference/random/generated/numpy.random.zipf.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Zipf's law
# MAGIC [Zipf's law](https://simple.wikipedia.org/wiki/Zipf%27s_law) is an empirical law, formulated using mathematical statistics, named after the linguist George Kingsley Zipf, who first proposed it.[1][2] Zipf's law states that given a large sample of words used, the frequency of any word is inversely proportional to its rank in the frequency table. So word number n has a frequency proportional to 1/n.
# MAGIC 
# MAGIC Thus the most frequent word will occur about twice as often as the second most frequent word, three times as often as the third most frequent word, etc. For example, in one sample of words in the English language, the most frequently occurring word, "the", accounts for nearly 7% of all the words (69,971 out of slightly over 1 million). True to Zipf's Law, the second-place word "of" accounts for slightly over 3.5% of words (36,411 occurrences), followed by "and" (28,852). Only about 135 words are needed to account for half the sample of words in a large sample.[3]
# MAGIC 
# MAGIC The same relationship occurs in many other rankings, unrelated to language, such as the population ranks of cities in various countries, corporation sizes, income rankings, etc. The appearance of the distribution in rankings of cities by population was first noticed by Felix Auerbach in 1913.[4]

# COMMAND ----------

# DBTITLE 1,Use Numpy to generate Zipf distribution
import numpy.random
import pandas as pd


def create_view(alpha:float = 1.5, p:int = 6, view_name:str = 'a'):
  """
  Purpose:
    Create dataframe with a Zipf distribution of keys to simulate real-world distributions.
  Inputs:
    alpha - The shape of the Zipf distribution, reverts to uniform random distribution when alpha <= 1.0
    p - is the exponent of 10 for the number of records to generate
    view_name - Is the temporary spark sql view to create
  Outputs:
    x - The 'database key' value, where the count of the key value has the zipf distribution.
    index - from Pandas
  """
  n = 10**p # breaks at 10**9

  if alpha > 1.0:
    x = numpy.random.zipf(alpha, n)
  else:
    x = numpy.random.randint(low=1, high=n, size=n)
    
  pdf = pd.DataFrame(data=x, columns=list('x'))
  pdf.reset_index(drop=False, inplace=True)
  df = spark.createDataFrame(pdf).repartition(200)
  df.createOrReplaceTempView(view_name)
  print(F"create view '{view_name}'")
  return df
help(create_view)

# COMMAND ----------

# MAGIC %md ## Setup and characterization

# COMMAND ----------

# DBTITLE 1,Generate test data
create_view(alpha=0, p=8, view_name='large')
create_view(alpha = 1.1, p=9, view_name='medium_skewed')
create_view(alpha = 1.0001, p=5, view_name='small_skewed')

# extreme skew
#create_view(alpha = 1.5, p=8, view_name='medium_skewed')
#create_view(alpha = 1.5, p=5, view_name='small_skewed')

# COMMAND ----------

# DBTITLE 1,Small Skewed
# MAGIC %sql select x, log10(count(1)) log10_count from small_skewed group by 1 order by 2 desc

# COMMAND ----------

# DBTITLE 1,Medium Skewed
# MAGIC %sql select x, log10(count(1)) log10_count from medium_skewed group by 1 order by 2 desc

# COMMAND ----------

# DBTITLE 1,large
# MAGIC %sql select x, log10(count(1)) log10_count 
# MAGIC from large group by 1
# MAGIC having count(1) > 1
# MAGIC order by 2 desc, 1 asc

# COMMAND ----------

# MAGIC %md ## Query tests

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled = false;
# MAGIC set spark.sql.shuffle.partitions = 200;
# MAGIC select count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN  small_skewed b
# MAGIC     ON a.x = b.x

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled = false;
# MAGIC set spark.sql.shuffle.partitions = 2000;
# MAGIC select count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN  small_skewed b
# MAGIC     ON a.x = b.x

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled = true;
# MAGIC set spark.sql.shuffle.partitions = 2000;
# MAGIC 
# MAGIC select count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN  small_skewed b
# MAGIC     ON a.x = b.x

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled = true;
# MAGIC set spark.sql.shuffle.partitions = 2000;
# MAGIC set spark.databricks.adaptive.skewJoin.enabled = true;
# MAGIC set spark.databricks.skewJoin.skewedPartitionThresholdInBytes = 1000;
# MAGIC set spark.databricks.skewJoin.skewedPartitionFactor = 2;
# MAGIC select count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN  small_skewed b
# MAGIC     ON a.x = b.x

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled = true;
# MAGIC set spark.sql.shuffle.partitions = 2000;
# MAGIC set spark.sql.adaptive.skewJoin.enabled = true;
# MAGIC set spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 1024;
# MAGIC set spark.sql.adaptive.skewJoin.skewedPartitionFactor = 2;
# MAGIC set spark.sql.adaptive.coalescePartitions.enabled = true;
# MAGIC set spark.sql.adaptive.advisoryPartitionSizeInBytes = 5120;
# MAGIC set spark.sql.adaptive.coalescePartitions.minPartitionSize = 1024;
# MAGIC set spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin = 0.2; -- 0.2 default
# MAGIC select count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN  small_skewed b
# MAGIC     ON a.x = b.x
# MAGIC where a.index > 0 
# MAGIC   and b.index > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.optimizer.adaptive.enabled = true;
# MAGIC set spark.sql.shuffle.partitions = 2000;
# MAGIC set spark.sql.adaptive.skewJoin.enabled = true;
# MAGIC set spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 1024;
# MAGIC set spark.sql.adaptive.skewJoin.skewedPartitionFactor = 2;
# MAGIC set spark.sql.adaptive.coalescePartitions.enabled = true;
# MAGIC set spark.sql.adaptive.advisoryPartitionSizeInBytes = 5120;
# MAGIC set spark.sql.adaptive.coalescePartitions.minPartitionSize = 1024;
# MAGIC set spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin = 0.2; -- 0.2 default
# MAGIC 
# MAGIC select /*+ BROADCAST(b) */ count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN  small_skewed b
# MAGIC     ON a.x = b.x
# MAGIC where a.index > 0 
# MAGIC   and b.index > 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN small_skewed b
# MAGIC     ON a.x = b.x

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.adaptive.skewJoin.enabled = true;
# MAGIC select /*+ SKEW('b') */ count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN  small_skewed b
# MAGIC     ON a.x = b.x

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.adaptive.skewJoin.enabled = false;
# MAGIC select /*+ BROADCAST(b) */ count(1) 
# MAGIC from  medium_skewed a
# MAGIC   JOIN  small_skewed b
# MAGIC     ON a.x = b.x
