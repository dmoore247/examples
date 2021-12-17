# Databricks notebook source
# MAGIC %md ## Databricks E2 network connectivity test, 
# MAGIC A stand alone python script for verifying connections between Databricks Data Plane and Databricks control plane and required AWS services.
# MAGIC 1. Download this as a .py python script
# MAGIC 2. Set the region parameter to the probe call.
# MAGIC 3. python3 ./net-debug.py

# COMMAND ----------

region = 'us-east-1'

# COMMAND ----------

# MAGIC %md
# MAGIC <table border="1">
# MAGIC <colgroup >
# MAGIC <col width="25%">
# MAGIC <col width="25%">
# MAGIC <col width="25%">
# MAGIC <col width="25%">
# MAGIC </colgroup>
# MAGIC <thead valign="bottom">
# MAGIC <tr class="row-odd"><th class="head">Endpoint</th>
# MAGIC <th class="head">VPC region</th>
# MAGIC <th class="head">Address</th>
# MAGIC <th class="head">Port</th>
# MAGIC </tr>
# MAGIC </thead>
# MAGIC <tbody valign="top">
# MAGIC <tr class="row-even"><td><strong>Webapp</strong></td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-northeast-1</span></code></td>
# MAGIC <td>tokyo.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-south-1</span></code></td>
# MAGIC <td>mumbai.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-southeast-1</span></code></td>
# MAGIC <td>singapore.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-southeast-2</span></code></td>
# MAGIC <td>sydney.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ca-central-1</span></code></td>
# MAGIC <td>canada.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-central-1</span></code></td>
# MAGIC <td>frankfurt.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-west-1</span></code></td>
# MAGIC <td>ireland.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-west-2</span></code></td>
# MAGIC <td>london.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-east-1</span></code></td>
# MAGIC <td>nvirginia.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-east-2</span></code></td>
# MAGIC <td>ohio.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-west-1</span></code></td>
# MAGIC <td>oregon.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-west-2</span></code></td>
# MAGIC <td>oregon.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td><strong>SCC relay</strong></td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-northeast-1</span></code></td>
# MAGIC <td>tunnel.ap-northeast-1.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-south-1</span></code></td>
# MAGIC <td>tunnel.ap-south-1.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-southeast-1</span></code></td>
# MAGIC <td>tunnel.ap-southeast-1.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-southeast-2</span></code></td>
# MAGIC <td>tunnel.ap-southeast-2.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ca-central-1</span></code></td>
# MAGIC <td>tunnel.ca-central-1.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-central-1</span></code></td>
# MAGIC <td>tunnel.eu-central-1.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-west-1</span></code></td>
# MAGIC <td>tunnel.eu-west-1.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-west-2</span></code></td>
# MAGIC <td>tunnel.eu-west-2.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-east-1</span></code></td>
# MAGIC <td>tunnel.us-east-1.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-east-2</span></code></td>
# MAGIC <td>tunnel.us-east-2.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-west-1</span></code></td>
# MAGIC <td>tunnel.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-west-2</span></code></td>
# MAGIC <td>tunnel.cloud.databricks.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td><strong>S3 global</strong> for&nbsp;root&nbsp;bucket</td>
# MAGIC <td><em>all</em></td>
# MAGIC <td>s3.amazonaws.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td><strong>S3 regional</strong> for&nbsp;other&nbsp;buckets: Databricks recommends a VPC&nbsp;endpoint instead</td>
# MAGIC <td><em>all</em></td>
# MAGIC <td>s3.&lt;region-name&gt;.amazonaws.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td><strong>STS global</strong></td>
# MAGIC <td><em>all</em></td>
# MAGIC <td>sts.amazonaws.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td><strong>Kinesis</strong>: Databricks recommends a VPC&nbsp;endpoint instead</td>
# MAGIC <td>Most regions</td>
# MAGIC <td>kinesis.&lt;region-name&gt;.amazonaws.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-west-1</span></code></td>
# MAGIC <td>kinesis.us-west-2.amazonaws.com</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td><strong>RDS (if using built-in metastore)</strong></td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-northeast-1</span></code></td>
# MAGIC <td>mddx5a4bpbpm05.cfrfsun7mryq.ap-northeast-1.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-south-1</span></code></td>
# MAGIC <td>mdjanpojt83v6j.c5jml0fhgver.ap-south-1.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-southeast-1</span></code></td>
# MAGIC <td>md1n4trqmokgnhr.csnrqwqko4ho.ap-southeast-1.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-southeast-2</span></code></td>
# MAGIC <td>mdnrak3rme5y1c.c5f38tyb1fdu.ap-southeast-2.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ca-central-1</span></code></td>
# MAGIC <td>md1w81rjeh9i4n5.co1tih5pqdrl.ca-central-1.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-central-1</span></code></td>
# MAGIC <td>mdv2llxgl8lou0.ceptxxgorjrc.eu-central-1.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-west-1</span></code></td>
# MAGIC <td>md15cf9e1wmjgny.cxg30ia2wqgj.eu-west-1.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-west-2</span></code></td>
# MAGIC <td>mdio2468d9025m.c6fvhwk6cqca.eu-west-2.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-east-1</span></code></td>
# MAGIC <td>mdb7sywh50xhpr.chkweekm4xjq.us-east-1.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-east-2</span></code></td>
# MAGIC <td>md7wf1g369xf22.cluz8hwxjhb6.us-east-2.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-west-1</span></code></td>
# MAGIC <td>mdzsbtnvk0rnce.c13weuwubexq.us-west-1.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-west-2</span></code></td>
# MAGIC <td>mdpartyyphlhsp.caj77bnxuhme.us-west-2.rds.amazonaws.com</td>
# MAGIC <td>3306</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td><strong>Databricks control plane infrastructure</strong></td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-northeast-1</span></code></td>
# MAGIC <td>35.72.28.0/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-south-1</span></code></td>
# MAGIC <td>65.0.37.64/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-southeast-1</span></code></td>
# MAGIC <td>13.214.1.96/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ap-southeast-2</span></code></td>
# MAGIC <td>3.26.4.0/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">ca-central-1</span></code></td>
# MAGIC <td>3.96.84.208/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-west-1</span></code></td>
# MAGIC <td>3.250.244.112/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-west-2</span></code></td>
# MAGIC <td>18.134.65.240/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">eu-central-1</span></code></td>
# MAGIC <td>18.159.44.32/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-east-1</span></code></td>
# MAGIC <td>3.237.73.224/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-even"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-east-2</span></code></td>
# MAGIC <td>3.128.237.208/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC <tr class="row-odd"><td>&nbsp;</td>
# MAGIC <td><code class="docutils literal notranslate"><span class="pre">us-west-1</span></code> and <code class="docutils literal notranslate"><span class="pre">us-west-2</span></code></td>
# MAGIC <td>44.234.192.32/28</td>
# MAGIC <td>443</td>
# MAGIC </tr>
# MAGIC </tbody>
# MAGIC </table>

# COMMAND ----------

import requests
import socket

def tcp_ping(host, port=443):
  timeout_value = 3 #seconds
  socket.setdefaulttimeout(timeout_value)
  if port > 0:
    try:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      # now connect to the web server on port 80 - the normal http port
      s.connect((host,port))
      msg = b'hello'
      s.send(msg)
      s.detach()
      s.close()
      return 'Success'
    except BaseException as e:
      print(F"Host: {host} incurs {e}")
      return 'Failure'
  else:
    return 'N/A'

def nslookup(host):
  try:
    ip_list = []
    ais = socket.getaddrinfo(host,0,0,0,0)
    for result in ais:
      ip_list.append(result[-1][0])
    ip_list = list(set(ip_list))
    #print (host, ip_list)
    return 'Success'
  except BaseException as e:
    print(F"Host: {host} incurs {e}")
    
def probe(region):
  print (F"Endpoint    \t\tnslookup | tcp_ping")
  print (F"--------    \t\t-------- | --------")
  for endpoint in endpoints[region]:
    print( F"{endpoint[0]}\t\t {nslookup(endpoint[1])} | {tcp_ping(endpoint[1],endpoint[2])}")

# COMMAND ----------

endpoints = {
  'us-west-1':
  [
    ('webapp   ',     'oregon.cloud.databricks.com', 443),
    ('SCC relay',     'tunnel.cloud.databricks.com', 443),
    ('S3 global',     's3.amazonaws.com', 443),
    ('Regional S3',   's3.us-west-1.amazonaws.com', 443),
    ('Kinesis    ',   'kinesis.us-west-1.amazonaws.com', 443),
    ('RDS        ',	  'mdzsbtnvk0rnce.c13weuwubexq.us-west-1.rds.amazonaws.com',	3306),
    ('Control Plane', '44.234.192.32',	-1)
  ],
  'us-east-1':
  [
    ('webapp   ',     'oregon.cloud.databricks.com', 443),
    ('SCC relay',     'tunnel.us-east-1.cloud.databricks.com', 443),
    ('S3 global',     's3.amazonaws.com', 443),
    ('Regional S3',   's3.us-east-1.amazonaws.com', 443),
    ('Kinesis    ',   'kinesis.us-east-1.amazonaws.com', 443),
    ('RDS        ',	  'mdb7sywh50xhpr.chkweekm4xjq.us-east-1.rds.amazonaws.com',	3306),
    ('Control Plane', '3.237.73.224',	-1)
  ]
 }
endpoints

# COMMAND ----------

# insert your region here, update region data structure above if not listed.
probe(region)

# COMMAND ----------


