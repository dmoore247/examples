# Databricks notebook source
# MAGIC %md # MRF files
# MAGIC 
# MAGIC As described under CMS price transparency guide: https://github.com/CMSgov/price-transparency-guide
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MRF (Machine Readable Format) files are large data files being put out by Insurance cariers as mandated by new Healthcare transparency laws. Databricks customers need to process these files. This notebook will research challenging parts of this task.
# MAGIC 
# MAGIC We'll tackle this project by researching key topics, proof points then integrating it all together into a usable & reusable package
# MAGIC 
# MAGIC ## Research Topics
# MAGIC - Splitting large multi-line json objects
# MAGIC   - Use the 'worlds fastest json parser' https://arxiv.org/abs/1902.08318 with it's python wrapper cysimdjson
# MAGIC - Download large .json.gz files using multiple threads
# MAGIC - Integrating it all into spark for multi-node performance
# MAGIC - Integrating it further with MTJ/DLT for monthly automation
# MAGIC 
# MAGIC ## Sample Data
# MAGIC Cigna Index located on this page: https://www.cigna.com/legal/compliance/machine-readable-files

# COMMAND ----------

# MAGIC %md ### Split large multi-line JSON payloads into JSONL consumable by Spark

# COMMAND ----------

# MAGIC %pip install cysimdjson

# COMMAND ----------

cigna_index = """https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/table-of-contents/reporting_month=2022-08/2022-08-01_cigna-health-life-insurance-company_index.json?Expires=1663258504&Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cHM6Ly9kMjVrZ3o1cmlra3E0bi5jbG91ZGZyb250Lm5ldC9jb3N0X3RyYW5zcGFyZW5jeS9tcmYvdGFibGUtb2YtY29udGVudHMvcmVwb3J0aW5nX21vbnRoPTIwMjItMDgvMjAyMi0wOC0wMV9jaWduYS1oZWFsdGgtbGlmZS1pbnN1cmFuY2UtY29tcGFueV9pbmRleC5qc29uIiwiQ29uZGl0aW9uIjp7IkRhdGVMZXNzVGhhbiI6eyJBV1M6RXBvY2hUaW1lIjoxNjYzMjU4NTA0fX19XX0_&Signature=NucG2ID8F7zsGtIqNirj1uliIPIuFuhEapXIC3MjTN4cjvDwoJiZ0X2-4PRERH7i0Y-T99~xUFBsO~NjkegP4R2HGgcZygAT6C5T6NHl1UY-~qowDIl3KnujEvNJLvxOEYftbZsE7yfpPWXlV8sqM5dvItJrRuQEhP6Du9kBYA~SifOtfLUz-a6wn9QdVbsfPo80mUHqq~OBuk3HOJigBJbS0miiUHRhvEbTdMa9Nu5VSwMLTrod850P~kh~TgzEB4MTP-B-PrarwUCgsu4aYP3Eh2OMSIy4kxnL8xtlhBL7W0EiUUlpvVgsOTUScp43eyGC0Mmi5LMnEwqLD8HJsg__&Key-Pair-Id=K1NVBEPVH9LWJP"""

# COMMAND ----------

import requests
r = requests.get(cigna_index)
r

# COMMAND ----------

import cysimdjson
parser = cysimdjson.JSONParser()
cp = parser.parse(str.encode(r.text))
cp.export()

# COMMAND ----------

len(cp.at_pointer('/reporting_structure'))

# COMMAND ----------

m = map(lambda x: str(x.export())+'\n', cp.at_pointer('/reporting_structure'))
l = [x for x in m]
len(l)

# COMMAND ----------

with open('/dbfs/tmp/mrf_reporting_structure.json','w') as f:
  f.writelines(l)

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -lh /dbfs/tmp/mrf_reporting_structure.json
# MAGIC 
# MAGIC wc /dbfs/tmp/mrf_reporting_structure.json

# COMMAND ----------

df = spark.read.json("dbfs:/tmp/mrf_reporting_structure.json")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md ## Optimize massive downloads of .json.gz files
# MAGIC 
# MAGIC Take for example an in-network download file. It is 10's of GB, gzipped JSON data. The example above from Cigna may be typical of the problem.
# MAGIC To accelerate downloads, the typical strategy is to download the file in multiple parts. http protocol supports file download with specified offset into the file.
# MAGIC the `lftp` program supports multi-part file download. `lftp` does require random write access filesystem. DBFS is sequential write filesystem.

# COMMAND ----------

# MAGIC %md
# MAGIC https://unix.stackexchange.com/questions/428233/download-big-file-over-bad-connection
# MAGIC ```
# MAGIC lftp -c 'set net:idle 10
# MAGIC          set net:max-retries 0
# MAGIC          set net:reconnect-interval-base 3
# MAGIC          set net:reconnect-interval-max 3
# MAGIC          pget -n 10 -c "https://host/file.tar.gz"'
# MAGIC ```

# COMMAND ----------

# MAGIC %sh apt-get install lftp -y

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/mrf-files /tmp/mrf-files

# COMMAND ----------

# MAGIC %md #### Run lftp
# MAGIC `lftp` generates a progress file which is updated with each thread running, starting offset and ending offset.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /tmp/mrf-files
# MAGIC lftp -v -c 'set net:idle 10
# MAGIC          set net:max-retries 0 
# MAGIC          set net:reconnect-interval-base 3
# MAGIC          set net:reconnect-interval-max 3
# MAGIC          pget -n 50 -c "https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/in-network-rates/reporting_month=2022-08/2022-08-01_cigna-health-life-insurance-company_national-oap_in-network-rates.json.gz?Expires=1661592274&Signature=XJXpeJ4V0hLUThc1pEjrNrjlQlkEwaq7EIv3ictz5mkBcQUPNmaNG7B2lBP0ABNxxd88i63g3e6mDb0ie8s~94XWEQaNs5Xkhyn1jOUb~Z4QJR-SKJwWQCM73N21oWq0MEIL5vhNXIN1xCd-Fl1Y9pHx-IpAq9omPN3Er7sMpDM~8wyvUAeKjJ69RObRP1H2lAhWivkbMhGIenE0mXHb0RpQdMXMszkKv22QR1kzSNfD-HZZH0Jg8ieokR7vAl-nQzZ2Na1D8GfD53ToAuJ6IgL0XP4GcK6AfhI709DlEXMKZB3QRqt6FBPTRBop-m6jHg6FnW3vX7brbb8mTGMm-g__&Key-Pair-Id=K1NVBEPVH9LWJP" -o 2022-08-01_cigna-health-life-insurance-company_national-oap_in-network-rates.json.gz'

# COMMAND ----------

# MAGIC %sh df -h

# COMMAND ----------

# MAGIC %fs mkdirs dbfs:/tmp/mrf-files

# COMMAND ----------

# MAGIC %fs cp file:///tmp/mrf-files/2022-08-01_cigna-health-life-insurance-company_national-oap_in-network-rates.json.gz dbfs:/tmp/mrf-files/

# COMMAND ----------


