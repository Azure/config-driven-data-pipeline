
from pyspark.sql.types import *

def start_ingestion_task(task, spark):
    schema = StructType.fromJson(task["schema"])
    fileConf = {}
    #add options from task options
    for key, value in task["options"].items():
        fileConf[key] = value

    if task["read-type"] == "Batch":
        df = spark.read.format("delta") \
            .options(**fileConf) \
            .schema(schema) \
            .load(task["path"])
        return df, False
    else:
        df = spark.readStream.format("delta") \
            .options(**fileConf) \
            .schema(schema) \
            .load(task["path"])
        return df, True
    
    