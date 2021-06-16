# Databricks notebook source
# MAGIC %md See Also https://eastus2.azuredatabricks.net/?o=984752964297111#notebook/4018666638949260/command/4018666638949261

# COMMAND ----------

import requests
import json

# COMMAND ----------

port = 8652 # Ganglia
host = 'localhost'
path = F'http://{host}:{port}/cluster'

r = requests.get(path)
(host,port,clusterId,r.content)

# COMMAND ----------

from xml.etree import ElementTree
from xml.parsers.expat import ExpatError
import re

# COMMAND ----------

def quote(s):
  return s

class Elem:
    def __init__(self, elem):
        self.elem = elem

    def __getattr__(self, name):
        return self.elem.get(name.upper())

class NullElem:
    def __getattr__(self, name):
        return None


class ApiMetric:
    tag_re = re.compile("\s+")

    def id(self):
        group = self.group if self.group is not None else ""
        id_elements = [self.environment, self.grid.name, self.cluster.name, self.host.name, group, self.name]
        return str.lower(".".join(filter(lambda e: e is not None, id_elements)))

    def api_dict(self):
        type, units = ApiMetric.metric_type(self.type, self.units, self.slope)
        metric = {'environment': self.environment,
                  'grid': self.grid.name,
                  'cluster': self.cluster.name,
                  'host': self.host.name,
                  'id': self.id(),
                  'metric': self.name,
                  'instance': self.instance,
                  'group': self.group,
                  'title': self.title,
                  'tags': ApiMetric.parse_tags(self.host.tags),
                  'description': self.desc,
                  'sum': self.sum,
                  'num': self.num,
                  'value': ApiMetric.is_num(self.val),
                  'units': units,
                  'type': type,
                  'sampleTime': datetime.datetime.fromtimestamp(
                      int(self.host.reported) + int(self.host.tn) - int(self.tn)).isoformat() + ".000Z",
                  'graphUrl': self.graph_url,
                  'dataUrl': self.data_url}
        return dict(filter(lambda k, v: v is not None, metric.items()))

    @staticmethod
    def parse_tags(tag_string):
        return tag_string.split(',')

    @staticmethod
    def metric_type(type, units, slope):
        if units == 'timestamp':
            return 'timestamp', 's'
        if 'int' in type or type == 'float' or type == 'double':
            return 'gauge', units
        if type == 'string':
            return 'text', units
        return 'undefined', units

    @staticmethod
    def is_num(val):
        try:
            return int(val)
        except ValueError:
            pass
        try:
            return float(val)
        except ValueError:
            return val

    def __str__(self):
        return "%s %s %s %s %s %s" % (
        self.environment, self.grid.name, self.cluster.name, self.host.name, self.group, self.name)


class Metric(Elem, ApiMetric):
    def __init__(self, elem, host, cluster, grid, environment):
        self.host = host
        self.cluster = cluster
        self.grid = grid
        self.environment = environment
        Elem.__init__(self, elem)
        self.metadata = dict()
        for extra_data in elem.findall("EXTRA_DATA"):
            for extra_elem in extra_data.findall("EXTRA_ELEMENT"):
                name = extra_elem.get("NAME")
                if name:
                    self.metadata[name] = extra_elem.get('VAL')
                    self.name = name

        original_metric_name = self.name

        try:
            self.metadata['NAME'], self.metadata['INSTANCE'] = self.name.split('-', 1)
        except ValueError:
            self.metadata['INSTANCE'] = ''

        if self.name in ['fs_util', 'inode_util']:
            if self.instance == 'rootfs':
                self.metadata['INSTANCE'] = '/'
            else:
                self.metadata['INSTANCE'] = '/' + '/'.join(self.instance.split('-'))

        params = {"environment": self.environment,
                  "grid": self.grid.name,
                  "cluster": self.cluster.name,
                  "host": self.host.name,
                  "metric": original_metric_name}
       # url = '%s%s/metrics?' % (settings.API_SERVER, settings.BASE_URL)
        url = "-"
        for (k, v) in params.items():
            if v is not None: url += "&%s=%s" % (k, quote(v))
        self.data_url = url

        params = {"c": self.cluster.name,
                  "h": self.host.name,
                  "v": "0",
                  "m": original_metric_name,
                  "r": "1day",
                  "z": "default",
                  "vl": self.units,
                  "ti": self.title}
        url = '%sgraph.php?' % self.grid.authority
        for (k, v) in params.items():
            if v is not None: url += "&%s=%s" % (k, quote(v))
        self.graph_url = url

    def __getattr__(self, name):
        try:
            if self.metadata.has_key(name.upper()):
                return self.metadata[name.upper()]
            else:
                return Elem.__getattr__(self, name)
        except AttributeError:
            return None

    def html_dir(self):
        return 'ganglia-' + self.environment + '-' + self.grid.name

class HeartbeatMetric(ApiMetric):
    def __init__(self, host, cluster, grid, environment):
        self.host = host
        self.cluster = cluster
        self.grid = grid
        self.environment = environment
        self.val = int(host.tn)
        self.tn = 0
        self.tags = host.tags
        self.name = "heartbeat"
        self.group = "ganglia"
        self.title = "Ganglia Agent Heartbeat"
        self.desc = "Ganglia agent heartbeat in seconds"
        self.type = 'uint16'
        self.units = 'seconds'
        self.slope = 'both'

    def __getattr__(self, name):
        return None

# COMMAND ----------



# COMMAND ----------

def host(elem):
  h = lambda e: elem.get(e)
  return {
            'NAME': h('NAME'),
            'IP': h('IP'),
            'REPORTED': h('REPORTED')
          }

def metric(elem):
  m = lambda e: elem.get(e)
  return {
            'NAME': m('NAME'),
            'VAL':  m('VAL'),
            'TYPE': m('TYPE'),
            'UNITS':  m('UNITS'),
          }

def record(h_elem, m_elem):
  h = lambda e: h_elem.get(e)
  m = lambda e: m_elem.get(e)
  return {
    'IP': h('IP'),
    'REPORTED': h('REPORTED'),
    'NAME': m('NAME'),
    'VAL':  m('VAL'),
    'TYPE': m('TYPE'),
    'UNITS':  m('UNITS')
  }

# COMMAND ----------

def parse_ganglia(content):
  ganglia = ElementTree.XML(content)
  for grid_elem in ganglia.findall("GRID"):
      #print('grid',grid_elem.items())
      for cluster_elem in grid_elem.findall("CLUSTER"):
          #print('cluster',cluster_elem.items())
          cluster = Elem(cluster_elem)
          for host_elem in cluster_elem.findall("HOST"):
              for metric_elem in host_elem.findall("METRIC"):
                  yield record(host_elem, metric_elem)
g = parse_ganglia(r.content)
for _ in g:
  print( _ )

# COMMAND ----------

df = spark.createDataFrame(parse_ganglia(r.content))
display(
  df.filter("NAME == 'cpu_idle'")
)

# COMMAND ----------


