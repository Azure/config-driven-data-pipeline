# Databricks notebook source
dbutils.fs.ls("/FileStore")
dbutils.fs.mkdirs("/FileStore/cddp/app_a")
dbutils.fs.mkdirs("/FileStore/cddp/app_a/storage/landing/price")
dbutils.fs.mkdirs("/FileStore/cddp/app_a/storage/landing/sales")

# COMMAND ----------

dbutils.fs.rm("/FileStore/cddp/app_a/storage/serving", recurse=True)

# COMMAND ----------

df = spark.read.format("delta").load("/FileStore/cddp/app_a/storage/serving/fruit_sales_total")
df.show()

# COMMAND ----------

df = spark.read.format("delta").load("/FileStore/cddp/app_a/storage/serving/nyc_taxi_dataset_curation_1")
df.show()

# COMMAND ----------


