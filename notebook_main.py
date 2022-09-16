# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT config_path DEFAULT "/dbfs/FileStore/cddp/app_a/pipeline.json";
# MAGIC CREATE WIDGET TEXT landing_path DEFAULT "/FileStore/cddp/app_a/storage/";
# MAGIC CREATE WIDGET TEXT serving_path DEFAULT "/FileStore/cddp/app_a/storage/serving/";
# MAGIC CREATE WIDGET TEXT task_id DEFAULT "";

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

import json
import os 
import sys 
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# COMMAND ----------

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
            .schema(schema) \
            .load(landing_path+"/"+location)    
        df.createOrReplaceTempView(target)

    elif type == "batch":
        df = spark \
            .read \
            .format("csv") \
            .option("multiline", "true") \
            .schema(schema) \
            .load(landing_path+"/"+location)  

        df.createOrReplaceTempView(target)
    
    else :
        raise Exception("Invalid type")
        

def start_standard_job(spark, config, name):
    """Creates the standard job"""
    sql = config["standard"][name]["sql"]
    target = config["standard"][name]["target"]
    df = spark.sql(sql)
    df.createOrReplaceTempView(target)
    

def start_serving_job(spark, config, name, timeout=None):
    """Creates the serving job"""
    sql = config["serving"][name]["sql"]
    target = config["serving"][name]["target"]
    format = config["serving"][name]["format"]
    df = spark.sql(sql)
    df.createOrReplaceTempView(target)

    query = df.writeStream\
            .format("delta") \
            .outputMode("complete")\
            .option("checkpointLocation", serving_path+"/"+target+"_chkpt")\
            .start(serving_path+"/"+target)

    if timeout is not None:
        query.awaitTermination(timeout)


# COMMAND ----------

def load_config(path) :
    """Loads the configuration file"""
    with open(path, 'r') as f:
        config = json.load(f)
    return config

# COMMAND ----------

config = load_config(config_path)
print(f"""{config["name"]} starting""")

# COMMAND ----------


for name in config["staging"]:
    if not task_id or name == task_id:
        start_staging_job(spark, config, name)
for name in config["standard"]:
    if not task_id or name == task_id:
        start_standard_job(spark, config, name)
for name in config["serving"]:
    if not task_id or name == task_id:
        start_serving_job(spark, config, name)

    
