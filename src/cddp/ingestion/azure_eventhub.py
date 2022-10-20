
from pyspark.sql.types import *

def start_ingestion_task(task, spark):
    import dbutils
    schema = StructType.fromJson(task["schema"])
    conn_str = dbutils.secrets.get(scope = task["secret_scope"], key = task["eventhubs_conn_str"])
    ehConf = {
        'eventhubs.connectionString' : conn_str
    }

    #add options from task options
    for key, value in task["options"].items():
        if key.startswith("eventhubs.") and key != "eventhubs.connectionString":
            ehConf[key] = value

    df = spark \
        .readStream \
        .format("eventhubs") \
        .options(**ehConf) \
        .schema(schema) \
        .load()

    return df, True