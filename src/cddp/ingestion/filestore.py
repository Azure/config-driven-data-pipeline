
from pyspark.sql.types import *

def start_ingestion_task(task, spark):
    schema = StructType.fromJson(task["schema"])
    fileConf = {}
    #add options from task options
    if 'options' in task['input'] and task['input']['options'] is not None:
        for key, value in task["input"]["options"].items():
            fileConf[key] = value

    if task["input"]["read-type"] == "batch":
        df = spark.read.format(task["input"]["format"]) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .options(**fileConf) \
            .schema(schema) \
            .load(task["input"]["path"])
        return df, False
    elif task["input"]["read-type"] == "streaming":
        df = spark.readStream.format(task["input"]["format"]) \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .options(**fileConf) \
            .schema(schema) \
            .load(task["input"]["path"])
        return df, True
    else:
        raise Exception("Unknown read-type: " + task["input"]["read-type"])
    