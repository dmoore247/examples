# Databricks notebook source
# MAGIC %md # Imaging Display Helper Demo
# MAGIC A class to 'automagically' recognize popular imaging object types, file paths and to render them in line and out of band using FileStore.
# MAGIC
# MAGIC Run `help(ImageDisplayHelper)` to get latest documentation

# COMMAND ----------

# DBTITLE 1,Setup custom class
# MAGIC %run "./Imaging Helper"

# COMMAND ----------

# MAGIC %md ## Install dependencies

# COMMAND ----------

# %sh apt-get install -y python3-openslide

# COMMAND ----------

# %pip install openslide-python

# COMMAND ----------

# MAGIC %md ## Tests

# COMMAND ----------

# MAGIC %md ### Slicer

# COMMAND ----------

# MAGIC %pip install image_slicer

# COMMAND ----------

path = "/dbfs/FileStore/plots/00011714-5f16-4bce-8ec6-8187c2c71f89.png"

# COMMAND ----------

from pyspark.ml.image import ImageSchema

# COMMAND ----------

from image_slicer import slice


#x1,x2, x3, x4 = x
#x1.image
#ImageDisplayHelper(x1.image)
def splitter(path: str):
  x = slice(path, col=2, row=2, save=False)
  for z in x:
    img = z.image
    print(type(img))
    yield { 
          'image': 
          [
          'file.png',
          img.height,
          img.width,
          4,
          24,
          img.copy().tobytes()
          ]
        }

# COMMAND ----------

images = [ x for x in splitter(path)]

# COMMAND ----------

df = spark.createDataFrame(images, ImageSchema.imageSchema)
df.createOrReplaceTempView("images")

# COMMAND ----------

# MAGIC %sql select image, md5(image.data) from images

# COMMAND ----------


print(type(images[0]['image']))
df = spark.createDataFrame(images, ImageSchema.imageSchema)
display(df)

# COMMAND ----------

# MAGIC %md ### Numpy ndarray -> bytes -> ndarray -> display

# COMMAND ----------

import numpy as np
from PIL import Image
from numpy import asarray
# load the image
image = Image.open(path)
# convert image to numpy array
array_out = asarray(image)
print(array_out.shape)
bytes = array_out.tobytes()

print(len(bytes))

array_in = np.asarray(bytes)
print(array_in.shape)

ImageDisplayHelper(array_in)

# COMMAND ----------

# MAGIC %md ### Numpy ndarray

# COMMAND ----------

import numpy as np
from PIL import Image
from numpy import asarray
# load the image
image = Image.open(path)
# convert image to numpy array
array = asarray(image)


ImageDisplayHelper(array)

# COMMAND ----------

def extract_blocks(a, blocksize, keep_as_view=False):
    M,N = a.shape
    b0, b1 = blocksize
    if keep_as_view==0:
        return a.reshape(M//b0,b0,N//b1,b1).swapaxes(1,2).reshape(-1,b0,b1)
    else:
        return a.reshape(M//b0,b0,N//b1,b1).swapaxes(1,2)

# COMMAND ----------


a = np.array([[1,5,9,13],
              [2,6,10,14],
              [3,7,11,15],
              [4,8,12,16]])

print(extract_blocks(a, (2, 2)))

# COMMAND ----------

# MAGIC %md ### Test Byte arrays from Images

# COMMAND ----------

path = "/dbfs/FileStore/plots/00011714-5f16-4bce-8ec6-8187c2c71f89.png"
with open(path,'rb') as fp:
  img_bytes = fp.read()
ImageDisplayHelper(img_bytes)

# COMMAND ----------

# MAGIC %md ### Test .png files

# COMMAND ----------

path = "/dbfs/FileStore/plots/00011714-5f16-4bce-8ec6-8187c2c71f89.png"
ImageDisplayHelper(path)

# COMMAND ----------

# MAGIC %md ### Test Image.Image types

# COMMAND ----------


path = "/dbfs/FileStore/plots/00011714-5f16-4bce-8ec6-8187c2c71f89.png"
img = Image.open(path)
ImageDisplayHelper(img)

# COMMAND ----------

# MAGIC %md ### Test Tiff files

# COMMAND ----------

# MAGIC %sh curl -o at3_1m4_01.tif https://people.math.sc.edu/Burkardt/data/tif/at3_1m4_01.tif

# COMMAND ----------

# MAGIC %sh ls -al

# COMMAND ----------

im = Image.open("at3_1m4_01.tif")
ImageDisplayHelper(im)

# COMMAND ----------

# MAGIC %sh curl -o mri.tif https://people.math.sc.edu/Burkardt/data/tif/mri.tif

# COMMAND ----------

# MAGIC %sh curl -o brain.tif https://people.math.sc.edu/Burkardt/data/tif/brain.tif

# COMMAND ----------

path = "/dbfs/databricks-datasets/med-images/camelyon16/normal_034.tif"
import numpy

 

# COMMAND ----------

# MAGIC %md #### OpenSlide

# COMMAND ----------

import numpy as np
import openslide
import matplotlib.pyplot as plt

f, axarr = plt.subplots(1,4,sharey=True)
i=0
for pid in ["normal_034","normal_036","tumor_044", "tumor_045"]:
  path = '/dbfs/%s/%s.tif' %(WSI_TIF_PATH,pid)
  slide = openslide.OpenSlide(path)
  axarr[i].imshow(slide.get_thumbnail(np.array(slide.dimensions)//50))
  axarr[i].set_title(pid)
  i+=1
display()

# COMMAND ----------

import openslide
import numpy as np

# COMMAND ----------

path = "/dbfs/databricks-datasets/med-images/camelyon16/normal_034.tif"
slide = openslide.OpenSlide(path)
for _ in slide.properties:
  print(_)

# COMMAND ----------

str(type(slide))

# COMMAND ----------

path = "/dbfs/databricks-datasets/med-images/camelyon16/normal_034.tif"
ImageDisplayHelper("/dbfs/databricks-datasets/med-images/camelyon16/normal_034.tif")

# COMMAND ----------

path = "/dbfs/databricks-datasets/med-images/camelyon16/normal_034.tif"
slide = openslide.OpenSlide(path)
dimensions = slide.dimensions
size = [500,500]
print(dimensions, size)

thumb = slide.get_thumbnail(size)
print(str(type(thumb)))
ImageDisplayHelper(thumb)

# COMMAND ----------

import os
os.stat(path)

# COMMAND ----------

slide.detect_format(path)

# COMMAND ----------

slide.level_dimensions

# COMMAND ----------

slide.properties

# COMMAND ----------


