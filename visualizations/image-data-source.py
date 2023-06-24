# Databricks notebook source
# MAGIC %md ## Image Data Source Sample
# MAGIC This sample notebook illustrates how to use the image data source. 
# MAGIC
# MAGIC #### Attribution
# MAGIC The source of these images are from video reenactment of a fight scene by CAVIAR members – [EC Funded CAVIAR project/IST 2001 37540](http://groups.inf.ed.ac.uk/vision/CAVIAR/CAVIARDATA1/).  The code used to generate these images can be found at [Identify Suspicious Behavior in Video with Databricks Runtime for Machine Learning](https://databricks.com/blog/2018/09/13/identify-suspicious-behavior-in-video-with-databricks-runtime-for-machine-learning.html).

# COMMAND ----------

# MAGIC %md ## Setup
# MAGIC

# COMMAND ----------

# Configure image paths
sample_img_dir = "/databricks-datasets/cctvVideos/train_images/"

# COMMAND ----------

# MAGIC %md ### Create Image DataFrame
# MAGIC
# MAGIC Create a DataFrame using image data source included in Apache Spark.  The image data source supports Hive-style partitioning, so if you upload images in the following structure:
# MAGIC * root_dir
# MAGIC   * label=0
# MAGIC     * image_001.jpg
# MAGIC     * image_002.jpg
# MAGIC     * ...
# MAGIC   * label=1
# MAGIC     * image_101.jpg
# MAGIC     * image_102.jpg
# MAGIC     * ...
# MAGIC
# MAGIC then the schema generated will be the following (via the `image_df.printSchema()` command)
# MAGIC
# MAGIC ```
# MAGIC root
# MAGIC  |-- image: struct (nullable = true)
# MAGIC  |    |-- origin: string (nullable = true)
# MAGIC  |    |-- height: integer (nullable = true)
# MAGIC  |    |-- width: integer (nullable = true)
# MAGIC  |    |-- nChannels: integer (nullable = true)
# MAGIC  |    |-- mode: integer (nullable = true)
# MAGIC  |    |-- data: binary (nullable = true)
# MAGIC  |-- label: integer (nullable = true)
# MAGIC ```

# COMMAND ----------

# Create image DataFrame using image data source
image_df = spark.read.format("image").load(sample_img_dir)

display(image_df) 

# COMMAND ----------

display(image_df.select("image.mode", "image.nChannels"))

# COMMAND ----------

# Test image_df.image.origin
display(image_df.select("image.origin"))

# COMMAND ----------

# Print schema of image_df
#  Note the label column based on the label=[0,1] within the file structure
image_df.printSchema()
