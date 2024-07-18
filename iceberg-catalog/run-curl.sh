# Test API using curl
# Usage: export DATABRICKS_TOKEN=<your workspace PAT token>
#

# works
echo "Get Config"
curl -i https://e2-demo-field-eng.cloud.databricks.com/api/2.1/unity-catalog/iceberg/v1/config \
     -H "Authorization: Bearer $DATABRICKS_TOKEN"
echo ""

# list namespaces
echo "List Namespaces"
curl -i "https://e2-demo-field-eng.cloud.databricks.com/api/2.1/unity-catalog/iceberg/v1/namespaces?parent=douglas_moore" \
     -H "Authorization: Bearer $DATABRICKS_TOKEN" \
     -H "Accept: */*" \
     -H "Content-type: application/json" \
     --output -
echo ""

# list tables
echo "List Tables"
curl -i https://e2-demo-field-eng.cloud.databricks.com/api/2.1/unity-catalog/iceberg/v1/namespaces/douglas_moore\%2Euniform/tables \
     -H "Authorization: Bearer $DATABRICKS_TOKEN" \
     -H "Accept: */*" \
     -H "Content-type: application/json" \
     --output -
echo ""
