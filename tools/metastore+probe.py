# Databricks notebook source
# MAGIC %md ## Probe the health of the default workspace metastore

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql.Connection
# MAGIC import java.sql.DriverManager
# MAGIC import java.sql.ResultSet
# MAGIC import java.sql.SQLException

# COMMAND ----------

# MAGIC %scala
# MAGIC /**
# MAGIC   * For details on what this query means, checkout https://dev.mysql.com/doc/refman/8.0/en/processlist-table.html
# MAGIC **/
# MAGIC
# MAGIC def printConnections: Unit = {
# MAGIC   val metastoreURL = spark.sparkContext.hadoopConfiguration.get("javax.jdo.option.ConnectionURL")
# MAGIC   val metastoreUser = spark.sparkContext.hadoopConfiguration.get("javax.jdo.option.ConnectionUserName")
# MAGIC   val metastorePassword = spark.sparkContext.hadoopConfiguration.get("javax.jdo.option.ConnectionPassword")
# MAGIC
# MAGIC   val connection = DriverManager.getConnection(metastoreURL, metastoreUser, metastorePassword)
# MAGIC   val statement = connection.createStatement()
# MAGIC   val resultSet = statement.executeQuery("SELECT HOST, COMMAND, STATE, INFO FROM INFORMATION_SCHEMA.PROCESSLIST ORDER BY Host")
# MAGIC
# MAGIC   val rsmd = resultSet.getMetaData();
# MAGIC   val columnsNumber = rsmd.getColumnCount();
# MAGIC   (1 to columnsNumber).foreach { i =>
# MAGIC     print(rsmd.getColumnName(i) + "\t")
# MAGIC   }
# MAGIC   println();
# MAGIC   while (resultSet.next()) {
# MAGIC       var cumulativeLength = 0
# MAGIC       (1 to columnsNumber).foreach { i =>
# MAGIC           val data = if (resultSet.getString(i) != null) resultSet.getString(i).trim() else ""
# MAGIC           print(data + "\t");
# MAGIC       }
# MAGIC       println();
# MAGIC   }
# MAGIC   statement.close
# MAGIC   connection.close
# MAGIC }
# MAGIC
# MAGIC printConnections

# COMMAND ----------


