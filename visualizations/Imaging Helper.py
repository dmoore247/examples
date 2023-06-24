# Databricks notebook source
# MAGIC %md # Imaging Display Helper
# MAGIC A class to 'automagically' recognize popular imaging object types, file paths and to render them in line and out of band using FileStore.
# MAGIC
# MAGIC Run `help(ImageDisplayHelper)` to get latest documentation

# COMMAND ----------

# DBTITLE 1,ImageDisplayHelper class
import base64
from PIL import Image
from io import BytesIO
from numpy import asarray, ndarray

class ImageDisplayHelper:
  """
    Extensible Display image file paths, byte arrays, file pointers, PIL images.
    Rendering is triggered when the notebook cell calls the ._repr_html_() method or the .as_html() method is called.
      Examples:
      
        # File Path
        ImageDisplayHelper('filepath.png')
        
        # File Descriptor
        with open('fliepath.png','rb') as f:
          bytes = f.read()
          ImageDisplayHelper(bytes)
          
         # PIL Image
        img = PIL.Image.open('filepath.png')
        ImageDisplayHelper(img)
        
        # Numpy.ndarray
        ImageDisplayHelper(array: ndarray)
  """
  def __init__(self, obj):
    self.obj = obj

  def _switcher(self):
    _switcher = {
      '<class \'str\'>': ImageDisplayHelper._display_image_file_path,
      '<class \'PIL.PngImagePlugin.PngImageFile\'>': ImageDisplayHelper._display_pil_image_inline,
      '<class \'_io.BufferedReader\'>': ImageDisplayHelper._display_image_fp,
      "<class '_io.BufferedReader'>":  ImageDisplayHelper._display_image_fp,
      "<class 'bytes'>": ImageDisplayHelper._display_bytes,
      "<class 'PIL.TiffImagePlugin.TiffImageFile'>": ImageDisplayHelper._display_pil_image_inline,
      "<class 'PIL.Image.Image'>": ImageDisplayHelper._display_pil_image_inline,
      "<class 'numpy.ndarray'>": ImageDisplayHelper._display_ndarray_inline,
      "<class \'bytearray\'>": ImageDisplayHelper._display_bytes
    }
    t = str(type(self.obj))
    print(F"type {t}")
    #print(_switcher.keys())
    fp = _switcher[t]
    print(t, fp)

    return fp(self.obj)

  def _display_bytes(bytes: bytearray):
    encoded = base64.b64encode(bytes).decode("UTF-8")
    return ((F'<p><img src=\'data:image/png;base64,{encoded}\'></p>'))
  
  def _display_pil_image_inline(im):
    b = BytesIO()
    im.save(b, format='png')
    bytes = b.getvalue()
    return ImageDisplayHelper._display_bytes(bytes)
  
  def _display_ndarray_inline(array: ndarray):
    return ImageDisplayHelper._display_pil_image_inline(Image.fromarray(array))
  
  def _display_pil_helper(path: str):
    from PIL.Image import io
    im = Image.open(path)
    return _display_pil_image_inline(im)
    
  def _display_image_fp(f):
      bytes = f.read()
      return ImageDisplayHelper._display_bytes(bytes)

  def _file_extension(path):
    return path.split('.')[-1]

  def _display_image_file_path(path):
    ext = ImageDisplayHelper._file_extension(path)
    if ext in ['tif', 'tiff']:
      return _display_pil_helper(path)

    if ext in ['png', 'jpg', 'jpeg']:
      with open(path,'rb') as f:
        return ImageDisplayHelper._display_image_fp(f)
    return F"<p>Unknown image extension {path}"

  def _repr_html_(self):
    return ImageDisplayHelper._switcher(self)
  
  def as_html(self):
    """Return Image object as HTML"""
    return ImageDisplayHelper._repr_html_(self)

help(ImageDisplayHelper)

# COMMAND ----------


