import json
import os 
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sys import argv

current_dir_path = os.path.dirname(os.path.realpath(__file__))
storage_dir_path = current_dir_path+"/storage"
landing_path = storage_dir_path+"/landing"
staging_path = storage_dir_path+"/staging"
standard_path = storage_dir_path+"/standard"
serving_path = storage_dir_path+"/serving"

def create_spark_session(config):
    """Creates a Spark Session"""
    conf = SparkConf().setAppName(config["name"]).setMaster("local[*]")
    spark = SparkSession.builder.config(conf=conf)\
        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.driver.memory", "4G")\
        .config("spark.driver.maxResultSize", "4G")\
        .getOrCreate()
    return spark

def create_folders():
    """Creates the folders for the data storage"""
    if not os.path.exists(staging_path):
        os.makedirs(staging_path)
    if not os.path.exists(standard_path):
        os.makedirs(standard_path)
    if not os.path.exists(serving_path):
        os.makedirs(serving_path)


def load_config(config_path) :
    """Loads the configuration file"""
    with open(f"{config_path}", 'r') as f:
        config = json.load(f)
    return config

def start_staging_job(spark, config, name, timeout=None):
    """Creates the staging job"""
    schema = StructType.fromJson(config["staging"][name]["schema"])
    location = config["staging"][name]["location"]
    target = config["staging"][name]["target"]
    type = config["staging"][name]["type"]
    output = config["staging"][name]["output"]
    format = config["staging"][name]["format"]
    if type == "streaming":
        df = spark \
            .readStream \
            .format(format) \
            .option("multiline", "true") \
            .option("header", "true") \
            .schema(schema) \
            .load(landing_path+"/"+location)    

        if "table" in output:
            query = df.writeStream\
                .format("delta") \
                .outputMode("append")\
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt") \
                .start(staging_path+"/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "view" in output:
            df.createOrReplaceTempView(target)

    elif type == "batch":
        df = spark \
            .read \
            .format(format) \
            .option("multiline", "true") \
            .option("header", "true") \
            .schema(schema) \
            .load(landing_path+"/"+location)  

        if "table" in output:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").save(staging_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)

    else :
        raise Exception("Invalid type")
        

def start_standard_job(spark, config, name, timeout=None):
    """Creates the standard job"""
    sql = config["standard"][name]["sql"]
    output = config["standard"][name]["output"]
    if(isinstance(sql, list)):
        sql = " \n".join(sql)
    target = config["standard"][name]["target"]

    load_staging_views()

    df = spark.sql(sql)

    type = "batch"
    if "type" in config["standard"][name]:
        type = config["standard"][name]["type"]

    if type == "streaming":

        if "table" in output:
            query = df.writeStream\
                .format("delta") \
                .outputMode("append")\
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format("delta") \
                .outputMode("append") \
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt") \
                .start(staging_path+"/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "view" in output:
            df.createOrReplaceTempView(target)

    elif type == "batch":

        if "table" in output:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").save(standard_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)
        
    else :
        raise Exception("Invalid type")


def start_serving_job(spark, config, name, timeout=None):
    """Creates the serving job"""
    sql = config["serving"][name]["sql"]
    output = config["serving"][name]["output"]
    if(isinstance(sql, list)):
        sql = " \n".join(sql)
    target = config["serving"][name]["target"]
    type = "batch"
    if "type" in config["serving"][name]:
        type = config["serving"][name]["type"]

    load_staging_views()
    load_standard_views()
    df = spark.sql(sql)

    if type == "streaming":
        if "table" in output:
            query = df.writeStream\
                .format("delta") \
                .outputMode("complete")\
                .option("checkpointLocation", serving_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format("delta") \
                .outputMode("complete") \
                .option("checkpointLocation", serving_path+"/"+target+"_chkpt") \
                .start(serving_path+"/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "view" in output:
            df.createOrReplaceTempView(target)

    elif type == "batch":
        if "table" in output:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(serving_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)
    else :
        raise Exception("Invalid type")

def load_staging_views():
    if 'staging' in config:
        for name in config["staging"]:
            schema = StructType.fromJson(config["staging"][name]["schema"])
            location = config["staging"][name]["location"]
            target = config["staging"][name]["target"]
            type = config["staging"][name]["type"]
            output = config["staging"][name]["output"]
            format = config["staging"][name]["format"]
            if type == "streaming" and "view" in output:
                df = spark \
                    .readStream \
                    .format(format) \
                    .option("multiline", "true") \
                    .option("header", "true") \
                    .schema(schema) \
                    .load(landing_path+"/"+location)    
                df.createOrReplaceTempView(target)
            elif type == "batch" and "view" in output:
                df = spark \
                    .read \
                    .format(format) \
                    .option("multiline", "true") \
                    .option("header", "true") \
                    .schema(schema) \
                    .load(landing_path+"/"+location)  
                df.createOrReplaceTempView(target)


def load_standard_views():
    if 'standard' in config:
        for name in config["standard"]:
            sql = config["standard"][name]["sql"]
            output = config["standard"][name]["output"]
            if(isinstance(sql, list)):
                sql = " \n".join(sql)
            target = config["standard"][name]["target"]
            df = spark.sql(sql)
            if "view" in output:
                df.createOrReplaceTempView(target)

def show_serving_dataset(spark, config, name):
    """Shows the serving dataset"""
    target = config["serving"][name]["target"]
    df = spark.read.format("delta").load(serving_path+"/"+target)
    df.show()

if __name__ == "__main__":
    config_path = argv[1];
    create_folders()
    config = load_config(config_path)
    print("app name: "+config["name"])
    spark = create_spark_session(config)
    if 'staging' in config:
        for name in config["staging"]:
            start_staging_job(spark, config, name, 50)
    if 'standard' in config:
        for name in config["standard"]:
            start_standard_job(spark, config, name, 100)
    if 'serving' in config:
        for name in config["serving"]:
            start_serving_job(spark, config, name, 150)