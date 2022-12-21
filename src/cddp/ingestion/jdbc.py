
from pyspark.sql.types import *
import IPython

def start_ingestion_task(task, spark):
    # import dbutils
    dbutils = IPython.get_ipython().user_ns["dbutils"]
    schema = StructType.fromJson(task["schema"])
    username = dbutils.secrets.get(scope = task["input"]["secret_scope"], key = task["input"]["jdbc_username"])
    password = dbutils.secrets.get(scope = task["input"]["secret_scope"], key = task["input"]["jdbc_password"])

    df = spark.read \
            .format("jdbc") \
            .option("url", task["input"]["jdbc_url"]) \
            .option("dbtable", task["input"]["table_name"]) \
            .option("user", username) \
            .option("password", password) \
            .schema(schema) \
            .load()
    
    return df, False