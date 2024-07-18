import logging

# These two lines enable debugging at httplib level (requests->urllib3->http.client)
# You will see the REQUEST, including HEADERS and DATA, and RESPONSE with HEADERS but without DATA.
# The only thing missing will be the response.body which is not logged.
try:
    import http.client as http_client
except ImportError:
    # Python 2
    import httplib as http_client

# set debug level (1 is debug, 0 is off)
http_client.HTTPConnection.debuglevel = 0

#set log level (ERROR, WARN, INFO, DEBUG)
log_level = logging.ERROR

# You must initialize logging, otherwise you'll not see debug output.
logging.basicConfig()
logging.getLogger().setLevel(log_level)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(log_level)
requests_log.propagate = True

#
# Purpose: List (Iceberg/Uniform) Tables managed by Databricks Unity Catalog
# Usage:
# % pip install pyiceberg==0.6.1
#
## Configure ~/.pyiceberg.yaml for uri, token and catalog type, example:
#
#catalog:
#  default:
#    uri: https://aws-workspace-name.cloud.databricks.com/api/2.1/unity-catalog/iceberg
#    token: dapi39d6blahpattokenblahblahblah
#    type: rest
#
## Create Iceberg/Uniform table:
#
# CREATE TABLE T(c1 INT) TBLPROPERTIES(
#  'delta.enableIcebergCompatV2' = 'true',
#  'delta.universalFormat.enabledFormats' = 'iceberg');
#
## Set myuccatalog value
#
# % python pyice-list-tables.py

from pyiceberg.catalog import load_catalog, Identifier
import time

myuccatalog = 'douglas_moore'

catalog = load_catalog("default")
print("connected", catalog)

# API lists only Iceberg tables for a namespace (A Unity Catalog catalog)
for ident in catalog.list_namespaces(namespace=myuccatalog):
    print('>>>> Namespace: ',ident)
    tables = catalog.list_tables(namespace=(F"{ident[1]}%2E{ident[2]}"))
    if len(tables) > 0:
        print('>>>>    Tables: ',tables)
    time.sleep(0.1) # API is currently rate limited to 5 calls/second

print('done.')
