from pyspark.sql.types import *

from datetime import datetime, timedelta
import time
import datetime

from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *

def start_ingestion_task(task, spark):
    from notebookutils import mssparkutils

    now = datetime.datetime.now()
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")

    ContainerName = task['input']["container_name"]
    FilePath = task['input']["data_folder"]
    FileName = time_str + "."+ task['input']["format"]
   
    client_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["application_id"]) 
    tenant_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["directory_id"]) 
    client_secret = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["service_credential_key"])
    # Azure subscription ID
    subscription_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["subscription_id"])

    # This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
    rg_name = task['input']["rg_name"]
    # The data factory name. It must be globally unique.
    df_name = task['input']["df_name"]
    # The ADF pipeline name
    p_name = task['input']["p_name"]

    token = task['input']["token"]
    params = {
        "ContainerName": ContainerName,
        "FilePath": FilePath,
        "FileName": FileName,
        "token": token,
    }

    # Specify your Active Directory client ID, client secret, and tenant ID
    credentials = ClientSecretCredential(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )
    adf_client = DataFactoryManagementClient(credentials, subscription_id)
    # Create a pipeline run
    run_response = adf_client.pipelines.create_run(
        rg_name, df_name, p_name, parameters=params
    )

    # Monitor the pipeline run
    pipeline_run_status = ""
    until_status = ["Succeeded", "TimedOut", "Failed", "Cancelled"]
    while pipeline_run_status not in until_status:
        pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)
        pipeline_run_status = pipeline_run.status
        print("\n\tPipeline run status: {}".format(pipeline_run.status))
        time.sleep(5)

    schema = StructType.fromJson(task["schema"])
    storage_account = task['input']["storage_account"]

    if "synapse_linkservice_name" in task['input']:
        spark.conf.set("spark.storage.synapse.linkedServiceName", task['input']["synapse_linkservice_name"])
        spark.conf.set("fs.azure.account.oauth.provider.type", "com.microsoft.azure.synapse.tokenlibrary.LinkedServiceBasedTokenProvider")
        path = f"abfss://{ContainerName}@{storage_account}.blob.core.windows.net/{FilePath}/{FileName}"
        df = spark.read.format(task['input']["format"]) \
            .option("header", "true") \
            .schema(schema) \
            .load(path)
        return df, False

    elif "sas-token" in task['input']:
        sas_token = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["storage_account"])
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.blob.core.windows.net", "SAS")
        spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.blob.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
        spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.blob.core.windows.net", sas_token)

    elif "storage_account_access_key" in task['input']:
        storage_account_access_key = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["storage_account_access_key"])
        spark.conf.set(f"fs.azure.account.key.{storage_account}.blob.core.windows.net", storage_account_access_key)
        
    elif "service_credential_key" in task['input']:
        application_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["application_id"]) 
        directory_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["directory_id"]) 
        service_credential = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["service_credential_key"])

        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.blob.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.blob.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.blob.core.windows.net", application_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.blob.core.windows.net", service_credential)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.blob.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

    path = f"wasbs://{ContainerName}@{storage_account}.blob.core.windows.net/{FilePath}/{FileName}"

    # read data of parquet, JSON, CSV, Text
    df = spark.read.format(task['input']["format"]) \
        .option("header", "true") \
        .schema(schema) \
        .load(path)

    return df, False
    

