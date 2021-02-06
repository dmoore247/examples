# Databricks notebook source
#%pip install aicspylibczi scikit-image

# COMMAND ----------

# DBTITLE 1,CZI Metadata
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, LongType, StringType, TimestampType
from pyspark.sql.functions import expr

from typing import Iterator
import pandas as pd
import numpy as np

# Image helper packages
from aicspylibczi import CziFile
from skimage import transform

def czi_meta(paths: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in paths:
      for i in range(pdf.shape[0]):
        print(F"czi_meta: pdf.shape:{pdf.shape}")
        path = pdf['path'][i]
        local_path = path.replace('dbfs:','/dbfs')  # convert distributed path to local path
        length = pdf['length'][i]
        modificationTime = pdf['modificationTime'][i]
        pdf['local_path'] = local_path

        print(F"opening {path}")
        try:
          czi = CziFile(local_path)
        except:
          print("CZI Read error")
          return "CZI Read error"
        dims = czi.dims_shape()
        is_mosaic = czi.is_mosaic()
        n_dims = len(dims)
        print("generating slice metadata")
        for scene in range(0,n_dims):
          for dim in dims[scene]:
            print(F"{path}: scene:{scene} dim:{dim}")
            pdf['is_mosaic'] = is_mosaic
            pdf['scene']  = scene
            pdf['dim'] = dim
            pdf['dim_index_min'] = dims[scene][dim][0]
            pdf['dim_index_max'] = dims[scene][dim][1]
            pdf['message'] = str(dims[scene][dim])
            yield pdf

czi_meta_schema = StructType([  
  StructField('path',StringType(),False),
  StructField('local_path',StringType(),False),
  StructField('length',LongType(),False),
  StructField('modificationTime',TimestampType(),False),
  StructField('is_mosaic', BooleanType(),False),
  StructField('scene', IntegerType(), False),
  StructField('dim', StringType(), False),
  StructField('dim_index_min',IntegerType(), False),
  StructField('dim_index_max',IntegerType(), False),
  StructField('message',StringType(), False),
])

# COMMAND ----------

# DBTITLE 1,CZI tiler function
from pyspark.sql.types import LongType, StringType, StructType, StructField, IntegerType, BinaryType
from pyspark.sql.functions import expr

from typing import Iterator
import pandas as pd
import numpy as np

# Image helper packages
from aicspylibczi import CziFile
from skimage import transform
from PIL import Image
from io import BytesIO

def get_img(local_path:str, C_index:int, M_index:int, scene:int):
    try:
      czi = CziFile(local_path)
      subblock = czi.read_subblock_rect(C=C_index,M=M_index,S=scene)
      mosaic_data = czi.read_mosaic(C=C_index, scale_factor=1.0, region=subblock)
    except Exception as err:
      print(F"path:{path} C_index:{C_index} scene:{scene} dim:{dim} M_index:{M_index} Err:{err}")
      return None

    img_in = mosaic_data[0,:,:]
    print(F"img_in {img_in.shape}, {img_in.dtype}, {np.info(img_in)}")
    return img_in

def tiler_input(pdf):
  for i in range(pdf.shape[0]):
    local_path = pdf['local_path'][i]
    scene = pdf['scene'][i]
    M_index = pdf['M_index'][i]
    C_index = pdf['C_index'][i]
    starts=pdf['starts'][i]
    
    prefix = 'tile'
    resize_shape = (1020, 1020) # down sampling by 2x  (508+512=1020)
    img_small_shape = (512, 512) for model training
    img_max_threshold = 25

    indx = M_index
    dim = pdf['dim'][i]
    print(F"Config: i{i} of {pdf.shape[0]}, M_index:{M_index}" )
    yield (local_path, scene, dim, M_index, C_index, starts, prefix, resize_shape, img_small_shape, img_max_threshold, i)

def czi_tiler(czimeta: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    j = 0
    for pdf in czimeta:
      for local_path, scene, dim, M_index, C_index, starts, prefix, resize_shape, img_small_shape, img_max_threshold, i in tiler_input(pdf):

        if dim not in 'M':
          continue

        img_in = get_img(local_path, C_index, M_index, scene)
        if img_in is None:
          continue

        # Check for black images
        if np.max( img_in ) < img_max_threshold:
          continue

        img_shrunk = transform.resize(img_in, resize_shape, preserve_range=True )
        img_shrunk = img_shrunk.astype(np.uint16)  # Native resolution of the image, for model training rescale 0.-1.0

        # Start tiling
        for xs in starts:
            for ys in starts:
                origin = F"{prefix}-j{j}-C{C_index}-{dim}{M_index}-S{scene}-x{xs}-y{ys}-CROP.png"
                print(F'origin: {origin}')

                img_small = img_shrunk[xs:(xs+img_small_shape[0]),ys:(ys+img_small_shape[1])] # returns numpy.ndarray
                j += 1
                b = BytesIO()
                Image.fromarray(img_small).save(b, format='png')
                pdf = pd.DataFrame(
                  {
                    'local_path': local_path,
                    'scene': scene,
                    'dim': dim,
                    'M_index': M_index,
                    'xs': xs,
                    'ys': ys,
                    'origin': origin,
                    'data':    img_small.tobytes(),
                    'shape':   str(img_small.shape),
                    'dtype':   str(img_small.dtype),
                    'png':     b.getvalue()
                  },
                  index = [0]
                )
                print(F'tiler: pdf.shape {pdf.shape}')
                yield pdf

                
czi_tiler_schema = StructType([
#    StructField('path',StringType(),False),
    StructField('local_path',StringType(),False),
#    StructField('length',LongType(),False),
#    StructField('modificationTime',TimestampType(),False),
#    StructField('is_mosaic', BooleanType(),False),
    StructField('scene', IntegerType(), False),
    StructField('dim', StringType(), False),
    StructField('M_index', IntegerType(), False),
#    StructField('message',StringType(), False),
    StructField('xs',IntegerType(),False),  # intermediate
    StructField('ys',IntegerType(),False),  # intermediate
    StructField('origin',StringType(),False),  # output
    StructField('data',BinaryType(),True),
#    StructField('ndarray',BinaryType(),True),
    StructField('shape',StringType(),True),
    StructField('dtype',StringType(),True),
    StructField('png',BinaryType(),True),
])

# COMMAND ----------

