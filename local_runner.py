import json
import os 
import sys 
import pandas as pd
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pathlib import Path  
import time

current_dir_path = os.path.dirname(os.path.realpath(__file__))
storage_dir_path = current_dir_path+"/storage"
serving_path = storage_dir_path+"/serving"

def load_config() :
    """Loads the configuration file"""
    with open(f"{current_dir_path}/pipeline.json", 'r') as f:
        config = json.load(f)
    return config

def create_spark_session(config):
    """Creates a Spark Session"""
    conf = SparkConf().setAppName(config["name"]).setMaster("local[*]")
    spark = SparkSession.builder.config(conf=conf)\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
        .config("spark.driver.memory", "4G")\
        .config("spark.driver.maxResultSize", "4G")\
        .getOrCreate()
    return spark

def create_folders():
    """Creates the folders for the data storage"""
    if not os.path.exists(storage_dir_path+"/staging"):
        os.makedirs(storage_dir_path+"/staging")
    if not os.path.exists(storage_dir_path+"/standard"):
        os.makedirs(storage_dir_path+"/standard")
    if not os.path.exists(storage_dir_path+"/serving"):
        os.makedirs(storage_dir_path+"/serving")

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
            .load(storage_dir_path+"/"+location)    
        df.createOrReplaceTempView(target)

    elif type == "batch":
        df = spark \
            .read \
            .format("csv") \
            .option("multiline", "true") \
            .schema(schema) \
            .load(storage_dir_path+"/"+location)  

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

    # create folder first to avoid conccurent issues
    if not os.path.exists(serving_path+"/"+target):
        os.makedirs(serving_path+"/"+target)
    if not os.path.exists(serving_path+"/"+target+"_chkpt"):
        os.makedirs(serving_path+"/"+target+"_chkpt")

    query = df.writeStream\
            .format("delta") \
            .outputMode("complete")\
            .option("checkpointLocation", serving_path+"/"+target+"_chkpt")\
            .start(serving_path+"/"+target)

    if timeout is not None:
        query.awaitTermination(timeout)



if __name__ == "__main__":

    create_folders()
    config = load_config()
    print("app name: "+config["name"])
    spark = create_spark_session(config)
    for name in config["staging"]:
        start_staging_job(spark, config, name)
    for name in config["standard"]:
        start_standard_job(spark, config, name)
    for name in config["serving"]:
        start_serving_job(spark, config, name, 20)
    