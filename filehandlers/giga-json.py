# Databricks notebook source
# MAGIC %pip install cysimdjson

# COMMAND ----------

# MAGIC %sh head /dbfs/tmp/movies.json

# COMMAND ----------

# MAGIC %sh ls -lh /dbfs/tmp/*.json

# COMMAND ----------

import cysimdjson

json_bytes = b'''
{
  "foo": [1,2,[3]]
}
'''

parser = cysimdjson.JSONParser()
json_element = parser.parse(json_bytes)

# Access using JSON Pointer
print(json_element.at_pointer("/foo/2/0"))

# COMMAND ----------

# MAGIC %sh head -10 /dbfs/tmp/movies.json

# COMMAND ----------

path = "/dbfs/tmp/movies.json"

parser = cysimdjson.JSONParser()

with open(path, 'rb') as f:
    jsonb = f.read()

parser = cysimdjson.JSONParser()

json_element = parser.parse(jsonb)

# Access using JSON Pointer
print(json_element.at_pointer("/2/title"))

# COMMAND ----------

len(json_element)

# COMMAND ----------

je = json_element.at_pointer("/2")

# COMMAND ----------

  l = len(json_element)
  print(json_element.at_pointer(F"/{l-1}").export())

# COMMAND ----------


print 
for i in range(len(json_element)):
  print(json_element.at_pointer(F"/{i}").export())

# COMMAND ----------

je.export()

# COMMAND ----------

# MAGIC %md # MRF files
# MAGIC Cigna Index located on this page: https://www.cigna.com/legal/compliance/machine-readable-files

# COMMAND ----------

cigna_index = """https://d25kgz5rikkq4n.cloudfront.net/cost_transparency/mrf/table-of-contents/reporting_month=2022-08/2022-08-01_cigna-health-life-insurance-company_index.json?Expires=1663258504&Policy=eyJTdGF0ZW1lbnQiOlt7IlJlc291cmNlIjoiaHR0cHM6Ly9kMjVrZ3o1cmlra3E0bi5jbG91ZGZyb250Lm5ldC9jb3N0X3RyYW5zcGFyZW5jeS9tcmYvdGFibGUtb2YtY29udGVudHMvcmVwb3J0aW5nX21vbnRoPTIwMjItMDgvMjAyMi0wOC0wMV9jaWduYS1oZWFsdGgtbGlmZS1pbnN1cmFuY2UtY29tcGFueV9pbmRleC5qc29uIiwiQ29uZGl0aW9uIjp7IkRhdGVMZXNzVGhhbiI6eyJBV1M6RXBvY2hUaW1lIjoxNjYzMjU4NTA0fX19XX0_&Signature=NucG2ID8F7zsGtIqNirj1uliIPIuFuhEapXIC3MjTN4cjvDwoJiZ0X2-4PRERH7i0Y-T99~xUFBsO~NjkegP4R2HGgcZygAT6C5T6NHl1UY-~qowDIl3KnujEvNJLvxOEYftbZsE7yfpPWXlV8sqM5dvItJrRuQEhP6Du9kBYA~SifOtfLUz-a6wn9QdVbsfPo80mUHqq~OBuk3HOJigBJbS0miiUHRhvEbTdMa9Nu5VSwMLTrod850P~kh~TgzEB4MTP-B-PrarwUCgsu4aYP3Eh2OMSIy4kxnL8xtlhBL7W0EiUUlpvVgsOTUScp43eyGC0Mmi5LMnEwqLD8HJsg__&Key-Pair-Id=K1NVBEPVH9LWJP"""

# COMMAND ----------

import requests
r = requests.get(cigna_index)
r

# COMMAND ----------

r.json()

# COMMAND ----------

import cysimdjson
parser = cysimdjson.JSONParser()
cp = parser.parse(str.encode(r.text))
cp.export()

# COMMAND ----------

len(cp.at_pointer('/reporting_structure'))

# COMMAND ----------

cp.at_pointer('/reporting_structure').export()

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


