
from pyspark.sql.types import *

def start_ingestion_task(task, spark):
    import dbutils
    schema = StructType.fromJson(task["schema"])
    storage_account = task["storage-account"]
    if "sas-token" in task:
        sas_token = dbutils.secrets.get(scope=task["secret_scope"], key=task["storage-account"])
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
        spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
        spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", sas_token)

    elif "service-credential-key" in task:

        application_id= dbutils.secrets.get(scope=task["secret_scope"], key=task["application-id"])
        directory_id= dbutils.secrets.get(scope=task["secret_scope"], key=task["directory-id"])
        service_credential = dbutils.secrets.get(scope=task["secret_scope"],key=task["service-credential-key"])

        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

    elif "storage-account-access-key" in task:
        storage_account_access_key = dbutils.secrets.get(scope=task["secret_scope"],key="storage-account-access-key")        
        spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_account_access_key)

    container_name = task["container-name"]
    path_to_data = task["data-folder"]
    path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{path_to_data}"

    # read data of parquet, JSON, CSV, Text
    df = spark.read.format(task["format"]) \
        .option("header", "true") \
        .schema(schema) \
        .load(path)

    return df, False
    