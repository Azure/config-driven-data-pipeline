import json
import os 
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from sys import argv
import shutil
from delta import *
import pyspark
from delta.tables import *

current_dir_path = os.path.dirname(os.path.realpath(__file__))
storage_dir_path = f"{current_dir_path}/data/storage"
storage_format = "delta"


def create_spark_session():
    """Creates a Spark Session"""
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark


def init(spark, config):
    """Delete the folders for the data storage"""
    app_name = config['name']
    config['landing_path'] = f"{current_dir_path}/data/landing/{app_name}"
    config['staging_path'] = f"{storage_dir_path}/{app_name}/staging"
    config['standard_path'] = f"{storage_dir_path}/{app_name}/standard"
    config['serving_path'] = f"{storage_dir_path}/{app_name}/serving"
    database_path = f"{current_dir_path}/spark-warehouse/{app_name}.db/"

    if os.path.exists(config['staging_path']):
        shutil.rmtree(config['staging_path'])
    if os.path.exists(config['standard_path']):
        shutil.rmtree(config['standard_path'])
    if os.path.exists(config['serving_path']):
        shutil.rmtree(config['serving_path'])
    if os.path.exists(database_path):
        shutil.rmtree(database_path)

    spark.sql(f"DROP SCHEMA IF EXISTS {app_name} CASCADE ")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {app_name}")
    spark.sql(f"USE SCHEMA {app_name}")


def load_config(config_path) :
    """Loads the configuration file"""
    with open(f"{config_path}", 'r') as f:
        config = json.load(f)
    return config


def start_staging_job(spark, config, name, timeout=None):
    """Creates the staging job"""
    app_name = config['name']
    schema = StructType.fromJson(config["staging"][name]["schema"])
    location = config["staging"][name]["location"]
    target = config["staging"][name]["target"]
    type = config["staging"][name]["type"]
    output = config["staging"][name]["output"]
    format = config["staging"][name]["format"]
    landing_path = config["landing_path"]
    staging_path = config["staging_path"]

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
                .format(storage_format) \
                .outputMode("append")\
                .option("checkpointLocation", staging_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format(storage_format) \
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
            df.write.format(storage_format).mode("append").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format(storage_format).mode("append").option("overwriteSchema", "true").save(staging_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)

    else :
        raise Exception("Invalid type")
        

def start_standard_job(spark, config, name, timeout=None):
    """Creates the standard job"""
       
    staging_path = config["staging_path"]
    standard_path = config["standard_path"]

    sql = config["standard"][name]["sql"]
    output = config["standard"][name]["output"]
    if(isinstance(sql, list)):
        sql = " \n".join(sql)
    target = config["standard"][name]["target"]

    load_staging_views(spark, config)

    df = spark.sql(sql)

    type = "batch"
    if "type" in config["standard"][name]:
        type = config["standard"][name]["type"]

    if type == "streaming":

        if "table" in output:
            query = df.writeStream\
                .format(storage_format) \
                .outputMode("append")\
                .option("checkpointLocation", standard_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format(storage_format) \
                .outputMode("append") \
                .option("checkpointLocation", standard_path+"/"+target+"_chkpt") \
                .start(standard_path+"/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "view" in output:
            df.createOrReplaceTempView(target)

    elif type == "batch":

        if "table" in output:
            df.write.format(storage_format).mode("append").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format(storage_format).mode("append").option("overwriteSchema", "true").save(standard_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)
        
    else :
        raise Exception("Invalid type")


def start_serving_job(spark, config, name, timeout=None):
    """Creates the serving job"""
    serving_path = config["serving_path"]
    sql = config["serving"][name]["sql"]
    output = config["serving"][name]["output"]
    if(isinstance(sql, list)):
        sql = " \n".join(sql)
    target = config["serving"][name]["target"]
    type = "batch"
    if "type" in config["serving"][name]:
        type = config["serving"][name]["type"]

    load_staging_views(spark, config)
    load_standard_views(spark, config)
    df = spark.sql(sql)

    if type == "streaming":
        if "table" in output:
            query = df.writeStream\
                .format(storage_format) \
                .outputMode("complete")\
                .option("checkpointLocation", serving_path+"/"+target+"_chkpt")\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "file" in output:
            query = df.writeStream \
                .format(storage_format) \
                .outputMode("complete") \
                .option("checkpointLocation", serving_path+"/"+target+"_chkpt") \
                .start(serving_path+"/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
        if "view" in output:
            df.createOrReplaceTempView(target)

    elif type == "batch":
        if "table" in output:
            df.write.format(storage_format).mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        if "file" in output:
            df.write.format(storage_format).mode("overwrite").option("overwriteSchema", "true").save(serving_path+"/"+target)
        if "view" in output:
            df.createOrReplaceTempView(target)
    else :
        raise Exception("Invalid type")

def load_staging_views(spark, config):
    landing_path = config["landing_path"]
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


def load_standard_views(spark, config):
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
    serving_path = f"{storage_dir_path}/{config['name']}/serving"
    target = config["serving"][name]["target"]
    df = spark.read.format(storage_format).load(serving_path+"/"+target)
    df.show()

def get_dataset_as_json(spark, config, stage, name, limit=20):
    """Shows the serving dataset"""
    staging_path = f"{storage_dir_path}/{config['name']}/staging"
    standard_path = f"{storage_dir_path}/{config['name']}/standard"
    serving_path = f"{storage_dir_path}/{config['name']}/serving"


    task = config[stage][name]
    task_type = task["type"]
    task_output = task["output"]
    if "view" in task_output and task_type != "streaming":
        target = task["target"]
        df = spark.sql("select * from "+target+" limit "+str(limit))
        return df.toJSON().map(lambda j: json.loads(j)).collect()
    elif "table" in task_output:
        target = task["target"]
        df = spark.sql("select * from "+target+" limit "+str(limit))
        return df.toJSON().map(lambda j: json.loads(j)).collect()
    elif "file" in task_output:
        target = task["target"]
        path = None
        if stage == "staging":
            path = staging_path            
        elif stage == "standard":
            path = standard_path
        elif stage == "serving":
            path = serving_path
        else:
            raise Exception("Invalid stage")
        df = spark.read.format(storage_format).load(path+"/"+target)
        df.createOrReplaceTempView("tmp_"+target)
        df = spark.sql("select * from tmp_"+target+ " limit "+str(limit))
        return df.toJSON().map(lambda j: json.loads(j)).collect()
    else:
        raise Exception("Invalid output")

if __name__ == "__main__":
    config_path = argv[1]
    timeout = 60
    if len(argv) > 2:
        timeout = int(argv[2])
    config = load_config(config_path)
    print("app name: "+config["name"]+", streaming job waiting for "+str(timeout)+" seconds")
    spark = create_spark_session()
    init(spark, config)
    if 'staging' in config:
        for name in config["staging"]:
            start_staging_job(spark, config, name, int(timeout))
    if 'standard' in config:
        for name in config["standard"]:
            start_standard_job(spark, config, name, int(timeout))
    if 'serving' in config:
        for name in config["serving"]:
            start_serving_job(spark, config, name, int(timeout))