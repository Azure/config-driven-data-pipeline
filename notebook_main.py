# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Add Widgets

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT config_path DEFAULT "/dbfs/FileStore/cddp/app_a/pipeline_fruit.json";
# MAGIC CREATE WIDGET TEXT landing_path DEFAULT "/FileStore/cddp/app_a/storage/";
# MAGIC CREATE WIDGET TEXT serving_path DEFAULT "/FileStore/cddp/app_a/storage/serving/";
# MAGIC CREATE WIDGET TEXT task_id DEFAULT "";

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Framework Functions

# COMMAND ----------

import json
import os 
import sys 
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

def start_staging_job(spark, config, name):
    """Creates the staging job"""
    schema = StructType.fromJson(config["staging"][name]["schema"])
    location = config["staging"][name]["location"]
    target = config["staging"][name]["target"]
    type = config["staging"][name]["type"]
    if type == "streaming":
        df = spark \
            .readStream \
            .format("csv") \
            .option("multiline", "true") \
            .option("header", "true") \
            .schema(schema) \
            .load(landing_path+"/"+location)  
        
        df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", f"/FileStore/cddp/{config['name']}/staging_chkpoints/{target}_chkpt") \
            .toTable(target)

    elif type == "batch":
        df = spark \
            .read \
            .format("csv") \
            .option("multiline", "true") \
            .option("header", "true") \
            .schema(schema) \
            .load(landing_path+"/"+location) 
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(target)
    
    else :
        raise Exception("Invalid type")
        

def start_standard_job(spark, config, name):
    """Creates the standard job"""
    sql = config["standard"][name]["sql"]
    if(isinstance(sql, list)):
        sql = " \n".join(sql)
    target = config["standard"][name]["target"]
    df = spark.sql(sql)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(target)
    

def start_serving_job(spark, config, name, timeout=None):
    """Creates the serving job"""
    sql = config["serving"][name]["sql"]
    if(isinstance(sql, list)):
        sql = " \n".join(sql)
    target = config["serving"][name]["target"]
    type = "batch"
    if "type" in config["serving"][name]:
        type = config["serving"][name]["type"]
    df = spark.sql(sql)



    if type == "streaming":
        query = df.writeStream\
                .format("delta") \
                .outputMode("complete")\
                .option("checkpointLocation", f"/FileStore/cddp/{config['name']}/serving_chkpoints/{target}_chkpt")\
                .start(serving_path+"/"+target)

        if timeout is not None:
            query.awaitTermination(timeout)
    else:
        df.write.format("delta").mode("overwrite").save(serving_path+"/"+target)

def load_config(path) :
    """Loads the configuration file"""
    with open(path, 'r') as f:
        config = json.load(f)
    return config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load arguments

# COMMAND ----------

config_path = getArgument("config_path")
landing_path = getArgument("landing_path")
serving_path = getArgument("serving_path")
task_id = getArgument("task_id")
print(f"""config_path {config_path}""")
print(f"""landing_path {landing_path}""")
print(f"""serving_path {serving_path}""")
print(f"""task_id {task_id}""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the pipeline

# COMMAND ----------

config = load_config(config_path)
print(f"""{config["name"]} starting""")

for name in config["staging"]:
    if not task_id or name == task_id:
        start_staging_job(spark, config, name)
for name in config["standard"]:
    if not task_id or name == task_id:
        start_standard_job(spark, config, name)
for name in config["serving"]:
    if not task_id or name == task_id:
        start_serving_job(spark, config, name)

    

# COMMAND ----------


