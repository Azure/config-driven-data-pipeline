from pyspark.sql.types import *
import IPython

def start_ingestion_task(task, spark):
    # import dbutils
    dbutils = IPython.get_ipython().user_ns["dbutils"]
    schema = StructType.fromJson(task["schema"])
    storage_account = task['input']["storage_account"]
    if "sas-token" in task['input']:
        sas_token = dbutils.secrets.get(scope=task['input']["secret_scope"], key=task['input']["storage_account"])
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
        spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
        spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", sas_token)

    elif "service_credential_key" in task['input']:

        application_id= dbutils.secrets.get(scope=task['input']["secret_scope"], key=task['input']["application_id"])
        directory_id= dbutils.secrets.get(scope=task['input']["secret_scope"], key=task['input']["directory_id"])
        service_credential = dbutils.secrets.get(scope=task['input']["secret_scope"],key=task['input']["service_credential_key"])

        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

    elif "storage_account_access_key" in task['input']:
        storage_account_access_key = dbutils.secrets.get(scope=task['input']["secret_scope"],key="storage_account_access_key")        
        spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_account_access_key)

    container_name = task['input']["container_name"]
    path_to_data = task['input']["data_folder"]
    path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{path_to_data}"

    # read data of parquet, JSON, CSV, Text
    df = spark.read.format(task['input']["format"]) \
        .option("header", "true") \
        .schema(schema) \
        .load(path)

    return df, False
    