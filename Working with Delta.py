# Databricks notebook source
# Python
df1 = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/parveen.raina@vnodeites.com/customerinfo.csv", header=True)


# COMMAND ----------


storageAccountName = "databrickst05022024"
appID = "d9360bb9-37b7-426b-a88b-d6d4110b57da"
secret = "OjH8Q~fr_5n-TabXtJOre2KmKdkKGA6Ecr05Oc.0"
fileSystemName = "data"
tenantID = "42f78800-9498-4c26-9548-8c7c50be6fea"

spark.conf.set("fs.azure.account.auth.type." + storageAccountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storageAccountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storageAccountName + ".dfs.core.windows.net", "" + appID + "")
spark.conf.set("fs.azure.account.oauth2.client.secret." + storageAccountName + ".dfs.core.windows.net", "" + secret + "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storageAccountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + tenantID + "/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://" + fileSystemName  + "@" + storageAccountName + ".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

df = spark.read.csv("abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/", header=True)

# COMMAND ----------

display(df)

# COMMAND ----------

# Python
df1.write.format("delta").save("/delta/customerinfo")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL
# MAGIC CREATE TABLE customerinfo
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/customerinfo'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL
# MAGIC SELECT * FROM customerinfo LIMIT 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL
# MAGIC UPDATE customerinfo SET Gender = 'Male' WHERE UserName = 'Jason.Green'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customerinfo WHERE UserName = 'Jason.Green'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL
# MAGIC DELETE FROM customerinfo WHERE UserName = 'Jason.Green'
# MAGIC
