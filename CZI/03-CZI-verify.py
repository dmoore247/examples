# Databricks notebook source
# MAGIC %run "./ImagingHelper.py"

# COMMAND ----------

delta_path = 'dbfs:/tmp/czi_tiles_dela'
tiles_df = spark.read.format('delta').load(delta_path)

# COMMAND ----------



# COMMAND ----------

ImageSchema.ocvTypes['CV_8UC4']

# COMMAND ----------

from pyspark.ml.image import ImageSchema
import hashlib

def to_image(data:bytes):
  """
  Expects PNG image bytes
  Param:
    bytes - PNG image bytes
    size  - Size of PNG image bytes
  """
  sig = hashlib.md5(data).hexdigest()

  b = BytesIO(initial_bytes=data)
  format = 'RGBA'
  r,g,b,a = Image.open(b).convert(format).split() # Convert to get matrix of pixel values
  imgx = Image.merge(format, (b, g, r, a)) # Flip color bands
  return {'image':[
            F'file-{sig}.png',
            imgx.height,
            imgx.width,
            4, # 
            ImageSchema.ocvTypes['CV_8UC4'],
            imgx.tobytes()
          ]}

# unit test
t_pdf = tiles_df.limit(1).toPandas()
png = t_pdf['png'][0]
spark_image = to_image(png)
img_df = spark.createDataFrame([spark_image],schema = ImageSchema.imageSchema)
display(img_df)

# COMMAND ----------

# DBTITLE 1,Create Spark UDF
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BinaryType
to_image_udf = udf(to_image, ImageSchema.imageSchema)

# COMMAND ----------

nd_df = spark.createDataFrame([png],BinaryType())
display(nd_df.select(to_image_udf(col('value')).alias('img')).select('img.image'))

# COMMAND ----------

# MAGIC %md ## Display data set

# COMMAND ----------

display(tiles_df.select(to_image_udf(col('png')).alias('img')).select('img.image'))

# COMMAND ----------

