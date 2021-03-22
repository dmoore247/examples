# Databricks notebook source
# MAGIC %md ## Download Dicom data

# COMMAND ----------

# MAGIC %sh wget ftp://dicom.offis.uni-oldenburg.de/pub/dicom/images/ddsm/benigns_01.zip

# COMMAND ----------

# MAGIC %sh unzip benigns_01.zip

# COMMAND ----------

# MAGIC %sh cp -r ./benigns /dbfs/FileStore/shared_uploads/douglas.moore@databricks.com/

# COMMAND ----------

# MAGIC %md ## Install

# COMMAND ----------

# MAGIC %pip install pydicom pillow

# COMMAND ----------

# MAGIC %conda install -c conda-forge gdcm

# COMMAND ----------

# MAGIC %pip install -vv -e git+https://github.com/dmoore247/ObjectFrames#egg=databricks.pixel

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

from databricks.pixel import ObjectFrames, Catalog
from databricks.pixel.tag_extractor import TagExtractor
from pydicom import dcmread
from pyspark.sql.functions import  col, array_contains

# COMMAND ----------

path = "dbfs:/FileStore/shared_uploads/douglas.moore@databricks.com/benigns/"
df = Catalog.catalog(spark, path)
display(df)

# COMMAND ----------

# MAGIC %md ## Pipeline

# COMMAND ----------

from pyspark.ml import Pipeline
#definition of object 
tex = TagExtractor(basePath = path)
xform =  Pipeline(stages=[tex])
x = xform.fit(df)
dfx = x.transform(df)
display(dfx)

# COMMAND ----------

# MAGIC %md ## Display single Dicom Image 

# COMMAND ----------

db_path = "dbfs:/FileStore/shared_uploads/douglas.moore@databricks.com/benigns/patient7186/7186.LEFT_MLO.dcm"
ds = dcmread(db_path.replace('dbfs:','/dbfs'))

# COMMAND ----------

ds

# COMMAND ----------

# DBTITLE 1,Plot one image
import matplotlib.pyplot as plt
from pydicom import dcmread

plt.imshow(ds.pixel_array, cmap="gray")
  
plt.show()
plt.close()

# COMMAND ----------

# DBTITLE 1,Filter full data set down to one patient, 4 images
dfx2 = dfx.filter(array_contains(col('tags'),'patient7186'))
display(dfx2)

# COMMAND ----------

# MAGIC %md ## Display multiple Mamograms

# COMMAND ----------

def get_loc(tags) -> int:
  """ Positioning logic """
  if 'LEFT' in tags:
    if 'CC'  in tags:
      return 1
    else:
      return 3
  else:
    if 'CC'  in tags:
      return 2
    else:
      return 4
  return -1

# COMMAND ----------

# DBTITLE 1,Pull from data frame
images = dfx2.select('path','tags').collect()

# COMMAND ----------

import matplotlib.pyplot as plt
from pydicom import dcmread

fig = plt.figure(figsize=(20,20))  # sets the window to 8 x 6 inches

for i in images:
  path = i[0].replace('dbfs:','/dbfs')
  ds = dcmread(path)
  print(ds)
  tags = i[1]
  loc = get_loc(tags)

  plt.subplot(2,2,loc)
  plt.imshow(ds.pixel_array, cmap="gray")
  
plt.show()
plt.close()

# COMMAND ----------

ds.Columns, ds.Rows

# COMMAND ----------

ds.LossyImageCompression

# COMMAND ----------

ds.__dir__()

# COMMAND ----------

# DBTITLE 1,Dicom metadata as JSON dictionary
js = ds.to_json_dict()
# remove binary images
del js['60003000']
del js['7FE00010']

#js['00082218']
js