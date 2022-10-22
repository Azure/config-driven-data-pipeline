
from pyspark.sql.types import *

def start_ingestion_task(task, spark):
    import dbutils
    schema = StructType.fromJson(task["schema"])
    username = dbutils.secrets.get(scope = task["secret_scope"], key = task["jdbc_username"])
    password = dbutils.secrets.get(scope = task["secret_scope"], key = task["jdbc_password"])

    spark.read \
        .format("jdbc") \
        .option("url", task["jdbc_url"]) \
        .option("dbtable", task["table_name"]) \
        .option("user", username) \
        .option("password", password) \
        .schema(schema) \
        .load()
    
    return df, False