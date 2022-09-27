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
import argparse
import time




storage_format = "delta"

def create_spark_session():
    """Creates a Spark Session"""
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def init(spark, config, working_dir):
    """Delete the folders for the data storage"""
    current_dir_path = os.path.dirname(os.path.realpath(__file__))
    if working_dir is None:
        working_dir = current_dir_path
    config['working_dir'] = working_dir

    app_name = config['name']
    config['app_data_path'] = f"{config['working_dir']}/{app_name}/"
    config['staging_path'] = f"{config['working_dir']}/{app_name}/staging"
    config['standard_path'] = f"{config['working_dir']}/{app_name}/standard"
    config['serving_path'] = f"{config['working_dir']}/{app_name}/serving"
    

    print(f"""app name: {config["name"]},
    landing path: {config['landing_path']},
    staging path: {config['staging_path']},
    standard path: {config['standard_path']},
    serving path: {config['serving_path']},
    working dir:{config['working_dir']},
    """)



def init_database(spark, config):
    app_name = config['name']
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {app_name}")
    spark.sql(f"USE SCHEMA {app_name}")

def clean_database(spark, config):
    app_name = config['name']
    current_dir_path = os.path.dirname(os.path.realpath(__file__))
    database_path = f"{current_dir_path}/spark-warehouse/{app_name}.db/"
    if os.path.exists(config['app_data_path']):
        shutil.rmtree(config['app_data_path'])
    if os.path.exists(database_path):
        shutil.rmtree(database_path)

    spark.sql(f"DROP SCHEMA IF EXISTS {app_name} CASCADE ")


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
    serving_path = f"{config['working-dir']}/{config['name']}/serving"
    target = config["serving"][name]["target"]
    df = spark.read.format(storage_format).load(serving_path+"/"+target)
    df.show()

def get_dataset_as_json(spark, config, stage, name, limit=20):
    """Shows the serving dataset"""
    staging_path = f"{config['working_dir']}/{config['name']}/staging"
    standard_path = f"{config['working_dir']}/{config['name']}/standard"
    serving_path = f"{config['working_dir']}/{config['name']}/serving"
    task = config[stage][name]
    task_type = task["type"]
    task_output = task["output"]
    app_name = config["name"]
    spark.sql(f"USE SCHEMA {app_name}")
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


def entrypoint():
    parser = argparse.ArgumentParser(description='Process Data pipeline')
    parser.add_argument('--config-path', help='path to pipeline config file', required=True)
    parser.add_argument('--landing-path', help='path to data landing zone', required=True)
    parser.add_argument('--working-dir', help='folder to store data of stages, the default value is a random tmp folder', required=False)
    parser.add_argument('--stage', help='run a task in the specified stage', required=False)
    parser.add_argument('--task', help='run a specified task', required=False)
    parser.add_argument('--show-result', type=bool, default=False, help='flag to show task data result', required=False)
    parser.add_argument('--await-termination', type=int, help='how many seconds to wait before streaming job terminating, no specified means not terminating.', required=False)

    args = parser.parse_args()

    config_path = args.config_path
    awaitTermination = args.await_termination
    stage = args.stage
    task = args.task
    working_dir = args.working_dir
    landing_path = args.landing_path
    show_result = args.show_result


    config = load_config(config_path)
    config['landing_path'] = landing_path
    config['working_dir'] = working_dir

    spark = create_spark_session()
   
    print(f"""app name: {config["name"]},
    config path: {config_path},
    landing path: {config['landing_path']},
    working dir:{config['working_dir']},
    stage: {stage},
    task: {task},   
    show_result: {show_result}, 
    streaming job waiting for {str(awaitTermination)} seconds before terminating
    """)

    init(spark, config, working_dir)
    init_database(spark, config)
    if 'staging' in config and (stage is None or stage == "staging"):
        for name in config["staging"]:
            if task is None or task == name:
                start_staging_job(spark, config, name, awaitTermination)
    if 'standard' in config and (stage is None or stage == "standard"):
        for name in config["standard"]:
            if task is None or task == name:
                start_standard_job(spark, config, name, awaitTermination)
    if 'serving' in config and (stage is None or stage == "serving"):
        for name in config["serving"]:
            if task is None or task == name:
                start_serving_job(spark, config, name, awaitTermination)
                if show_result:
                    print(get_dataset_as_json(spark, config, "serving", name))


def wait_for_next_stage():    
    parser = argparse.ArgumentParser(description='Wait for the next stage')
    parser.add_argument('--duration', type=int, default=10, help='how many seconds to wait', required=False)
    args = parser.parse_args()
    print(f"waiting for {args.duration} seconds to next stage")
    time.sleep(args.duration)

