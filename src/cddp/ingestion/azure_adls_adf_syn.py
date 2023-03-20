from pyspark.sql.types import *

from datetime import datetime, timedelta
import time
import datetime
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *


def start_ingestion_adf(task, spark):
    from notebookutils import mssparkutils

    now = datetime.datetime.now()
    time_str = now.strftime("%Y-%m-%d %H:%M:%S")

    ContainerName = task['input']["container_name"]
    FilePath = task['input']["data_folder"]
    FileName = time_str + ".txt"
    token = "mockdata/historical_data_time_2022Q2.txt?sp=r&st=2023-03-16T10:42:31Z&se=2023-07-20T18:42:31Z&spr=https&sv=2021-12-02&sr=b&sig=Q7eugzDoP1%2F9Z3w6l3HuPGFVvdBJCpmPC7%2FN38gGK5k%3D"

    client_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["application_id"]) 
    tenant_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["directory_id"]) 
    client_secret = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["service_credential_key"])
    # Azure subscription ID
    subscription_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["subscription_id"])
    # This program creates this resource group. If it's an existing resource group, comment out the code that creates the resource group
    rg_name = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["rg_name"])
    # The data factory name. It must be globally unique.
    df_name = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["df_name"])
    # The ADF pipeline name
    p_name = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["p_name"])

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
    if "sas-token" in task:
        sas_token = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["storage_account"])
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
        spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
        spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", sas_token)

    elif "service-credential-key" in task:

        application_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["application_id"]) 
        directory_id = mssparkutils.credentials.getSecret(task['input']["secret_scope"], task['input']["directory_id"]) 
        service_credential = mssparkutils.credentials.getSecret(task['input']["secret_scope"],task['input']["service_credential_key"])

        spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")
    
    elif "storage_account-access-key" in task:
        storage_account_access_key = mssparkutils.credentials.getSecret(task['input']["secret_scope"], "storage-account-access-key") 
        spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_account_access_key)

    container_name = task['input']["container_name"]
    path_to_data = task['input']["data_folder"]
    path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{path_to_data}"

    # read data of parquet, JSON, CSV, Text
    df = spark.read.format(task["input"]["format"]) \
        .option("header", "true") \
        .schema(schema) \
        .load(path)

    return df, False
    