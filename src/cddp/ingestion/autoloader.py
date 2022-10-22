
from pyspark.sql.types import *

def start_ingestion_task(task, spark):
    import dbutils
    schema = StructType.fromJson(task["schema"])

    autoLoaderConf = {}

    if "clientId" in task:
        autoLoaderConf["cloudFiles.clientId"] = dbutils.secrets.get(scope = task["secret_scope"], key = task["clientId"])
        autoLoaderConf["cloudFiles.clientSecret"] = dbutils.secrets.get(scope = task["secret_scope"], key = task["clientSecret"])
        autoLoaderConf["cloudFiles.tenantId"] = dbutils.secrets.get(scope = task["secret_scope"], key = task["tenantId"])
        autoLoaderConf["cloudFiles.subscriptionId"] = dbutils.secrets.get(scope = task["secret_scope"], key = task["subscriptionId"])
        autoLoaderConf["cloudFiles.resourceGroup"] = dbutils.secrets.get(scope = task["secret_scope"], key = task["resourceGroup"])
    elif "connectionString" in task:
        autoLoaderConf["cloudFiles.connectionString"] = dbutils.secrets.get(scope = task["secret_scope"], key = task["connectionString"])

    autoLoaderConf["cloudFiles.format"] = task["format"]

    #add options from task options
    for key, value in task["options"].items():
            autoLoaderConf[key] = value

    df = spark.readStream.format("cloudFiles") \
        .options(**autoLoaderConf) \
        .schema(schema) \
        .load(task["path"])
    return df, True
