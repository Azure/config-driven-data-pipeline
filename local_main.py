import json
import os 
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sys import argv

current_dir_path = os.path.dirname(os.path.realpath(__file__))
storage_dir_path = current_dir_path+"/storage"
staging_path = storage_dir_path+"/staging"
standard_path = storage_dir_path+"/standard"
serving_path = storage_dir_path+"/serving"

def load_config(pipeline_path) :
    """Loads the configuration file"""
    with open(f"{current_dir_path}/{pipeline_path}", 'r') as f:
        config = json.load(f)
    return config

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

def start_staging_job(spark, config, name, timeout=None):
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
            .load(storage_dir_path+"/"+location)    

        df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", staging_path+"/"+target+"_chkpt") \
            .toTable(target)

        


    elif type == "batch":
        df = spark \
            .read \
            .format("csv") \
            .option("multiline", "true") \
            .option("header", "true") \
            .schema(schema) \
            .load(storage_dir_path+"/"+location)  
            
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


    # create folder first to avoid conccurent issues
    if not os.path.exists(serving_path+"/"+target):
        os.makedirs(serving_path+"/"+target)
    if not os.path.exists(serving_path+"/"+target+"_chkpt"):
        os.makedirs(serving_path+"/"+target+"_chkpt")
    if type == "streaming":
        query = df.writeStream\
                .format("delta") \
                .outputMode("complete")\
                .option("checkpointLocation", serving_path+"/"+target+"_chkpt")\
                .start(serving_path+"/"+target)

        if timeout is not None:
            query.awaitTermination(timeout)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(serving_path+"/"+target)

def show_serving_dataset(spark, config, name):
    """Shows the serving dataset"""
    target = config["serving"][name]["target"]
    df = spark.read.format("delta").load(serving_path+"/"+target)
    df.show()

if __name__ == "__main__":
    pipeline_path = argv[1];
    create_folders()
    config = load_config(pipeline_path)
    print("app name: "+config["name"])
    spark = create_spark_session(config)
    for name in config["staging"]:
        start_staging_job(spark, config, name, 20)
    for name in config["standard"]:
        start_standard_job(spark, config, name)
    for name in config["serving"]:
        start_serving_job(spark, config, name, 40)
    