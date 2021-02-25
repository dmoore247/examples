#!/bin/bash -x
  
#
# import cert from $HOST and install cert chain into Java cert chain
#

#
# Replace with your databricks workspace host name:port
#
export HOST="demo.cloud.databricks.com:443"


# find JAVA_HOME
export JAVA_HOME=`$(dirname $(readlink $(which javac)))/java_home`

# export certificates from server
openssl s_client -connect $HOST <<<'' | openssl x509 -out /tmp/db-corp.crt

# perform backup
sudo cp $JAVA_HOME/jre/lib/security/cacerts "$JAVA_HOME/jre/lib/security/cacerts-$dt.bak"

#
# import cert into Java keystore
#
# Enter system password, then 'changeit'
sudo keytool -import -alias databricks_certificate -file /tmp/db-corp.crt -keystore  $JAVA_HOME/jre/lib/security/cacerts


# Reboot to have changes take place (for some services)
