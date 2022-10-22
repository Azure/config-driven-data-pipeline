
from pyspark.sql.types import *

def start_ingestion_task(task, spark):
    import dbutils
    schema = StructType.fromJson(task["schema"])
    

    application_id= dbutils.secrets.get(scope=task["secret_scope"], key=task["application-id"])
    directory_id= dbutils.secrets.get(scope=task["secret_scope"], key=task["directory-id"])
    key_name_for_service_credential = dbutils.secrets.get(scope=task["secret_scope"],key=task["key-name-for-service-credential"])
    storage_resource= task["storage-resource"]
    directory_name= task["directory-name"]
    

    spark.conf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential")
    spark.conf.set("fs.adl.oauth2.client.id", application_id)
    spark.conf.set("fs.adl.oauth2.credential", key_name_for_service_credential)
    spark.conf.set("fs.adl.oauth2.refresh.url", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

    path = f"adl://{storage_resource}.azuredatalakestore.net/{directory_name}"

    # read data of parquet, JSON, CSV, Text
    df = spark.read.format(task["format"]) \
        .option("header", "true") \
        .schema(schema) \
        .load(path)

    return df, False
    