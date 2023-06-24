# Databricks notebook source
# MAGIC %md
# MAGIC # Datashader - Install
# MAGIC ![](https://github.com/holoviz/datashader/blob/master/examples/assets/images/usa_census.jpg?raw=true)
# MAGIC
# MAGIC Last tested with 
# MAGIC `6.4 ML (includes Apache Spark 2.4.5, Scala 2.11)`

# COMMAND ----------

# DBTITLE 1,Download and install census2010 data
# MAGIC %sh
# MAGIC cd /tmp
# MAGIC wget http://s3.amazonaws.com/datashader-data/census2010.parq.zip
# MAGIC unzip /tmp/census2010.parq.zip
# MAGIC cp -r census2010.parq /dbfs/ml/census2010.parq

# COMMAND ----------

# MAGIC %md Cluster libraries
# MAGIC * bokeh==1.4.0
# MAGIC * datashader
# MAGIC * fastparquet
# MAGIC * holoviews
# MAGIC * python-snappy

# COMMAND ----------

# MAGIC %sh
# MAGIC /databricks/conda/bin/conda install geos cartopy geoviews -y

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC cat <<EOF >> requirements.txt
# MAGIC absl-py==0.9.0
# MAGIC asn1crypto==0.24.0
# MAGIC astor==0.8.0
# MAGIC backcall==0.1.0
# MAGIC bcrypt==3.1.7
# MAGIC bokeh==1.4.0
# MAGIC boto==2.49.0
# MAGIC boto3==1.9.162
# MAGIC botocore==1.12.163
# MAGIC certifi==2019.3.9
# MAGIC cffi==1.12.2
# MAGIC chardet==3.0.4
# MAGIC Click==7.0
# MAGIC cloudpickle==0.8.0
# MAGIC colorama==0.4.1
# MAGIC colorcet==2.0.2
# MAGIC configparser==3.7.4
# MAGIC cryptography==2.6.1
# MAGIC cudf-cuda92==0.6.1
# MAGIC cycler==0.10.0
# MAGIC Cython==0.29.6
# MAGIC dask==2.12.0
# MAGIC databricks-cli==0.9.1
# MAGIC datashader==0.10.0
# MAGIC datashape==0.5.2
# MAGIC decorator==4.4.0
# MAGIC Deprecated==1.2.7
# MAGIC distributed==2.12.0
# MAGIC docker==4.2.0
# MAGIC docutils==0.14
# MAGIC entrypoints==0.3
# MAGIC et-xmlfile==1.0.1
# MAGIC fastparquet==0.3.3
# MAGIC Flask==1.0.2
# MAGIC fsspec==0.6.2
# MAGIC fusepy==2.0.4
# MAGIC future==0.17.1
# MAGIC gast==0.2.2
# MAGIC geos==0.2.2
# MAGIC gitdb2==2.0.6
# MAGIC GitPython==2.1.11
# MAGIC google-pasta==0.1.8
# MAGIC gorilla==0.3.0
# MAGIC grpcio==1.16.1
# MAGIC gunicorn==19.9.0
# MAGIC h5py==2.9.0
# MAGIC HeapDict==1.0.1
# MAGIC holoviews==1.12.7
# MAGIC horovod==0.19.0
# MAGIC html5lib==1.0.1
# MAGIC hvplot==0.5.2
# MAGIC hyperopt===0.2.2.db1
# MAGIC idna==2.8
# MAGIC imageio==2.8.0
# MAGIC ipykernel==5.1.0
# MAGIC ipython==7.4.0
# MAGIC ipython-genutils==0.2.0
# MAGIC itsdangerous==1.1.0
# MAGIC jdcal==1.4
# MAGIC jedi==0.13.3
# MAGIC Jinja2==2.10
# MAGIC jmespath==0.9.4
# MAGIC jupyter-client==5.2.4
# MAGIC jupyter-core==4.4.0
# MAGIC Keras==2.2.5
# MAGIC Keras-Applications==1.0.8
# MAGIC Keras-Preprocessing==1.1.0
# MAGIC kiwisolver==1.0.1
# MAGIC llvmlite==0.28.0
# MAGIC locket==0.2.0
# MAGIC lxml==4.3.2
# MAGIC Mako==1.0.10
# MAGIC Markdown==3.1.1
# MAGIC MarkupSafe==1.1.1
# MAGIC matplotlib==3.0.3
# MAGIC mkl-fft==1.0.10
# MAGIC mkl-random==1.0.2
# MAGIC mleap==0.8.1
# MAGIC mlflow==1.5.0
# MAGIC msgpack==1.0.0
# MAGIC multipledispatch==0.6.0
# MAGIC networkx==2.2
# MAGIC nose==1.3.7
# MAGIC nose-exclude==0.5.0
# MAGIC numba==0.41.0
# MAGIC numpy==1.16.2
# MAGIC nvstrings-cuda92==0.3.0.post1
# MAGIC olefile==0.46
# MAGIC openpyxl==2.6.1
# MAGIC opt-einsum==3.1.0
# MAGIC packaging==20.3
# MAGIC pandas==0.24.2
# MAGIC param==1.9.3
# MAGIC paramiko==2.4.2
# MAGIC parso==0.3.4
# MAGIC partd==1.1.0
# MAGIC pathlib2==2.3.3
# MAGIC patsy==0.5.1
# MAGIC pexpect==4.6.0
# MAGIC pickleshare==0.7.5
# MAGIC Pillow==5.4.1
# MAGIC ply==3.11
# MAGIC prompt-toolkit==2.0.9
# MAGIC protobuf==3.11.4
# MAGIC psutil==5.6.1
# MAGIC psycopg2==2.7.6.1
# MAGIC ptyprocess==0.6.0
# MAGIC pyarrow==0.12.1
# MAGIC pyasn1==0.4.8
# MAGIC pycparser==2.19
# MAGIC pyct==0.4.6
# MAGIC Pygments==2.3.1
# MAGIC pymongo==3.8.0
# MAGIC PyNaCl==1.3.0
# MAGIC pyOpenSSL==19.0.0
# MAGIC pyparsing==2.3.1
# MAGIC PySocks==1.6.8
# MAGIC python-dateutil==2.8.0
# MAGIC python-editor==1.0.4
# MAGIC python-snappy==0.5.4
# MAGIC pytz==2018.9
# MAGIC pyviz-comms==0.7.4
# MAGIC PyWavelets==1.1.1
# MAGIC PyYAML==5.1
# MAGIC pyzmq==18.0.0
# MAGIC querystring-parser==1.2.4
# MAGIC requests==2.21.0
# MAGIC s3transfer==0.2.1
# MAGIC scikit-image==0.16.2
# MAGIC scikit-learn==0.20.3
# MAGIC scipy==1.2.1
# MAGIC seaborn==0.9.0
# MAGIC simplejson==3.16.0
# MAGIC singledispatch==3.4.0.3
# MAGIC six==1.12.0
# MAGIC smmap2==2.0.5
# MAGIC sortedcontainers==2.1.0
# MAGIC sqlparse==0.3.0
# MAGIC statsmodels==0.9.0
# MAGIC tabulate==0.8.3
# MAGIC tblib==1.6.0
# MAGIC tensorboard==1.15.0
# MAGIC tensorboardX==1.9
# MAGIC tensorflow==1.15.0
# MAGIC tensorflow-estimator==1.15.1
# MAGIC termcolor==1.1.0
# MAGIC thrift==0.13.0
# MAGIC toolz==0.10.0
# MAGIC torch==1.4.0
# MAGIC torchvision==0.5.0
# MAGIC tornado==6.0.2
# MAGIC tqdm==4.31.1
# MAGIC traitlets==4.3.2
# MAGIC typing-extensions==3.7.4.1
# MAGIC urllib3==1.24.1
# MAGIC virtualenv==16.0.0
# MAGIC wcwidth==0.1.7
# MAGIC webencodings==0.5.1
# MAGIC websocket-client==0.56.0
# MAGIC Werkzeug==0.14.1
# MAGIC wrapt==1.11.1
# MAGIC xarray==0.15.0
# MAGIC xgboost==0.90
# MAGIC zict==2.0.0
# MAGIC EOF

# COMMAND ----------

import bokeh
print(bokeh.__version__)

# COMMAND ----------


