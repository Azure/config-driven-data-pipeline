import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import shutil
from delta import *
from delta.tables import *
import argparse
import time
import tempfile
import uuid

import cddp.ingestion as cddp_ingestion
import cddp.utils as utils



storage_format = "delta"


def create_spark_session():
    """Creates a Spark Session"""
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('spark.sql.warehouse.dir', './tmp/my-spark-warehouse') 
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
    config['staging_path'] = f"{config['working_dir']}/{app_name}/stg/"
    config['standard_path'] = f"{config['working_dir']}/{app_name}/std/"
    config['serving_path'] = f"{config['working_dir']}/{app_name}/srv/"

    print(f"""app name: {config["name"]},
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
    database_path = f"./tmp/my-spark-warehouse/{app_name}.db/"

    if os.path.exists(config['app_data_path']):
        shutil.rmtree(config['app_data_path'], True)
    if os.path.exists(database_path):
        #delete folder database_path
        print("delete path: "+database_path)
        shutil.rmtree(database_path, True)
        

    spark.sql(f"DROP SCHEMA IF EXISTS {app_name} CASCADE ")


def load_config(config_path):
    """Loads the configuration file"""
    with open(f"{config_path}", 'r') as f:
        config = json.load(f)
    return config

def output_dataset(spark, task, df, is_streaming, path, mode="append", timeout=None):
    output_type = task["output"]["type"]
    target = task["output"]["target"]
    if is_streaming:
        if "table" in output_type:

            query = df.writeStream\
                .format(storage_format) \
                .outputMode(mode)\
                .option("checkpointLocation", path+"/chkpt/"+target)\
                .toTable(target)
            if timeout is not None:
                query.awaitTermination(timeout)
                query.stop()
        if "file" in output_type:
            query = df.writeStream \
                .format(storage_format) \
                .outputMode(mode) \
                .option("checkpointLocation", path+"/chkpt/"+target)\
                .start(path+"/data/"+target)
            if timeout is not None:
                query.awaitTermination(timeout)
                query.stop()
        if "view" in output_type:
            df.createOrReplaceTempView(target)
    else:
        if "table" in output_type:
            df.write.format(storage_format).mode(mode).option(
                "overwriteSchema", "true").saveAsTable(target)
        if "file" in output_type:
            df.write.format(storage_format).mode(mode).option(
                "overwriteSchema", "true").save(path+"/data/"+target)
        if "view" in output_type:
            df.createOrReplaceTempView(target)

def start_staging_job(spark, config, task, timeout=None):
    """Creates the staging job"""
    print(f"Starting staging job for {task['name']}\n{json.dumps(task)}")
    staging_path = config["staging_path"]
    df, is_streaming = cddp_ingestion.start_ingestion_task(task, spark)
    output_dataset(spark, task, df, is_streaming, staging_path, "append", timeout)


def start_standard_job(spark, config, task, need_load_views=True, test_mode=False, timeout=None):
    """Creates the standard job"""
    print(f"Starting standard job for {task['name']}\n{json.dumps(task)}")
    standard_path = config["standard_path"]
    # target = task["target"]
    if need_load_views:
        load_staging_views(spark, config)
    
    df = run_task_code(spark, task)
    if test_mode:
        output_dataset(spark, task, df, False, standard_path, "append", timeout)
    else:
        output_dataset(spark, task, df, task["type"]=="streaming", standard_path, "append", timeout)
    return df


def start_serving_job(spark, config, task, need_load_views=True, test_mode=False, timeout=None):
    """Creates the serving job"""
    print(f"Starting serving job for {task['name']}\n{json.dumps(task)}")
    serving_path = config["serving_path"]
    
    if need_load_views:
        load_staging_views(spark, config)
        load_standard_views(spark, config)
   
    df = run_task_code(spark, task)
    if task["type"]=="streaming" and not test_mode:
        output_dataset(spark, task, df, True, serving_path, "complete", timeout)
    else:
        output_dataset(spark, task, df, False, serving_path, "overwrite", timeout)

    

def run_task_code(spark, task):
    if task['code']['lang'] == "python" and "python" in task['code']:
        python_code = task['code']["python"]
        if (isinstance(python_code, list)):
            python_code = " \n".join(python_code)
        
        python_code+=f"\n\noutput_df.createOrReplaceTempView('__{task['name']}_code_output__')"
        exec(python_code)
        df = spark.sql(f"select * from __{task['name']}_code_output__")
    elif task['code']['lang'] == "sql" and "sql" in task['code']:
        sql = task['code']["sql"]
        if (isinstance(sql, list)):
            sql = " \n".join(sql)
        df = spark.sql(sql)
    return df

def load_staging_views(spark, config):   
    if 'staging' in config:
        for task in config["staging"]:
            output_type = task["output"]["type"]
            target = task["output"]["target"]
            if "view" in output_type:
                df, is_streaming = cddp_ingestion.start_ingestion_task(task, spark)
                df.createOrReplaceTempView(target)


def load_standard_views(spark, config):
    
    if 'standard' in config:
        for task in config["standard"]:
            output_type = task["output"]["type"]
            target = task["output"]["target"]
            if "view" in output_type:
                df =  start_standard_job(spark, config, task)
                df.createOrReplaceTempView(target)



def show_serving_dataset(spark, config, task):
    """Shows the serving dataset"""
    serving_path = f"{config['working-dir']}/{config['name']}/srv"
    target = task["target"]
    df = spark.read.format(storage_format).load(serving_path+"/"+target)
    df.show()


def get_dataset_as_json(spark, config, stage, task, limit=20):
    """Shows the serving dataset"""
    staging_path = f"{config['working_dir']}/{config['name']}/stg/data"
    standard_path = f"{config['working_dir']}/{config['name']}/std/data"
    serving_path = f"{config['working_dir']}/{config['name']}/srv/data"
    task_output = task["output"]["type"]
    target = task["output"]["target"]
    app_name = config["name"]
    spark.sql(f"USE SCHEMA {app_name}")
    if "view" in task_output:        
        df = spark.sql("select * from "+target+" limit "+str(limit))

    elif "table" in task_output:        
        df = spark.sql("select * from "+target+" limit "+str(limit))
        
    elif "file" in task_output:        
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
        df = spark.sql("select * from tmp_"+target + " limit "+str(limit))
        
    else:
        raise Exception("Invalid output")
    
    result = df.toJSON().map(lambda j: json.loads(j)).collect()
    return result, df


def entrypoint():
    parser = argparse.ArgumentParser(description='Process Data pipeline')
    parser.add_argument(
        '--config-path', help='path to pipeline config file', required=True)
    # parser.add_argument(
    #     '--landing-path', help='path to data landing zone', required=True)
    parser.add_argument(
        '--working-dir', help='folder to store data of stages, the default value is a random tmp folder', required=False)
    parser.add_argument(
        '--stage', help='run a task in the specified stage', required=False)
    parser.add_argument('--task', help='run a specified task', required=False)
    parser.add_argument('--show-result', type=bool, default=False,
                        help='flag to show task data result', required=False)
    parser.add_argument('--build-landing-zone', type=bool, default=False,
                        help='build landing zone and import sample data, it will create folder "FileStore" in root folder', required=False)
    parser.add_argument('--await-termination', type=int,
                        help='how many seconds to wait before streaming job terminating, no specified means not terminating.', required=False)
    parser.add_argument('--cleanup-database', type=bool, default=False,
                        help='Clean up existing database', required=False)

    args = parser.parse_args()

    config_path = args.config_path
    awaitTermination = args.await_termination
    stage_arg = args.stage
    task_arg = args.task
    working_dir = args.working_dir
    # landing_path = args.landing_path
    show_result = args.show_result
    build_landing_zone = args.build_landing_zone
    cleanup_database = args.cleanup_database

    config = load_config(config_path)
    # config['landing_path'] = landing_path
    config['working_dir'] = working_dir

    if 'spark' not in globals():
        spark = create_spark_session()

    print(f"""app name: {config["name"]},
    config path: {config_path},
    working dir:{config['working_dir']},
    stage: {stage_arg},
    task: {task_arg},   
    show_result: {show_result}, 
    streaming job waiting for {str(awaitTermination)} seconds before terminating
    """)


        
    init(spark, config, working_dir)

    if cleanup_database:
        clean_database(spark, config)

    init_database(spark, config)


    if build_landing_zone:
        create_landing_zone(config)

    if 'staging' in config and (stage_arg is None or stage_arg == "staging"):
        for task in config["staging"]:
            if task_arg is None or task['name'] == task_arg:
                start_staging_job(spark, config, task, awaitTermination)
    if 'standard' in config and (stage_arg is None or stage_arg == "standard"):
        for task in config["standard"]:
            if task_arg is None or task['name'] == task_arg:
                start_standard_job(spark, config, task, True, False, awaitTermination)
    if 'serving' in config and (stage_arg is None or stage_arg == "serving"):
        for task in config["serving"]:
            if task_arg is None or task['name'] == task_arg:
                start_serving_job(spark, config, task, True, False, awaitTermination)
                if show_result:
                    json, df = get_dataset_as_json(spark, config, "serving", task)
                    df.show()


def wait_for_next_stage():
    parser = argparse.ArgumentParser(description='Wait for the next stage')
    parser.add_argument('--duration', type=int, default=10,
                        help='how many seconds to wait', required=False)
    args = parser.parse_args()
    print(f"waiting for {args.duration} seconds to next stage")
    time.sleep(args.duration)


def load_sample_data(spark, data_str, format="json"):
    # save data_str to temp file
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.write(data_str.encode())
    temp_file.close()
    file_path = temp_file.name
    if format == "json":
        # read json file to dataframe
        df = spark \
            .read \
            .format("json") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(temp_file.name)
    elif format == "csv":
        df = spark \
            .read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiline", "true") \
            .load(file_path)
    # create random table name
    table_name = "tmp_"+str(uuid.uuid4()).replace("-", "")
    df.createOrReplaceTempView("tmp_"+table_name)
    df = spark.sql("select * from tmp_"+table_name + " limit "+str(25))
    data = df.toJSON().map(lambda j: json.loads(j)).collect()
    json_str = json.dumps(data)
    schema = df.schema.json()
    return json_str, schema


def init_staging_sample_dataframe(spark, config):
    staging_path = config["staging_path"]
    for task in config["staging"]:
        if 'sampleData' in task:
            target = task["output"]["target"]
            output = task["output"]["type"]
            task_landing_path = task["input"]["path"]
            if not os.path.exists(task_landing_path):
                os.makedirs(task_landing_path)
            filename = task['name']+".json"
            sampleData = task['sampleData']
            with open(task_landing_path+"/"+filename, "w") as text_file:
                json.dump(sampleData, text_file)
            schema = StructType.fromJson(task["schema"])
            df = spark \
                .read \
                .format("json") \
                .option("multiline", "true") \
                .option("header", "true") \
                .schema(schema) \
                .load(task_landing_path+"/"+filename)

            if "table" in output:
                df.write.format(storage_format).mode("append").option(
                    "overwriteSchema", "true").saveAsTable(target)
            if "file" in output:
                df.write.format(storage_format).mode("append").option(
                    "overwriteSchema", "true").save(staging_path+"/"+target)
            if "view" in output:
                df.createOrReplaceTempView(target)

def create_landing_zone(config):
    for task in config["staging"]:
        type = task["input"]["type"]            
        if type == "filestore":
            name = task["name"]
            format = task["input"]["format"]
            path = task["input"]["path"]
            if not os.path.exists(path):
                os.makedirs(path)
            sample_data = task["sampleData"]
            if format == "csv":
                data_path = f"{path}/{name}.csv"
                utils.json_to_csv(sample_data, data_path)
            elif format == "json":
                data_path = f"{path}/{name}.json"
                with open(data_path, 'w') as sample_data_outfile:
                    json.dump(sample_data, sample_data_outfile)