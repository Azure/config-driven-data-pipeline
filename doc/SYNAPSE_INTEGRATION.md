# Azure Synapse Analytics Integration
## Overview 

It is possible to create and deploy the CDDP pipeline to Azure Synapse Analytics and run the pipeline with Spark Pools. 
  
> "Azure Synapse Analytics is a limitless analytics service that brings together data integration, enterprise data warehousing, and big data analytics. It gives you the freedom to query data on your terms, using either serverless or dedicated options—at scale. "  

Some Features of Azure Synapse Analytics including:
- Perform data ingestion, exploration, analytics in one unified environment
- Deeply integrated Apache Spark and SQL engines
- Support multiple languages, including T-SQL, KQL, Python, Scala, Spark SQL, and .Net
- Built-in ability to explore data lake
- ...

Find more information about Azure Synapse Analytics from [here](https://azure.microsoft.com/en-us/products/synapse-analytics/#overview).


## Prerequisite

1. Create a Synapse workspace from Azure portal.
2. Go to the resouce page of workspace, create a new Apache Spark Pool.
3. Go to the resouce page of the Spark pool, under **packages**, install cddp package by uploading a *requirements.txt* which includes package name `cddp`.
![syn1.png](../images/syn1.png)
4. If you would like to test a specific version of cddp or with a local built wheel,  
    - Open Synapse Studio from workspace
    - Upload the wheel file to workspace
    ![syn2.png](../images/syn2.png)
    - Go back to pool page and add the package to pool by **Select from workspace packages**
5. Go back to the resouce page of the Spark pool, and add following Spark configurations.
![syn3.png](../images/syn3.png)
```
{
    "spark.cddp.synapse.storageAccountName": "[storage account name]",
    "spark.cddp.synapse.fileSystemName": "[file system name]",
    "spark.cddp.synapse.linkedService": "[linked service name]"
}
```
You can use the settings when creating the Synapse workspace (and check them in the Synapse Studio), or you can also use newly added linked service of a storage account to the workspace.

## Create a Spark Job Definition Manually

1. Upload the main definition file to the linked storage account above (you can use **src/main.py** as the main definition file).
```
import cddp
import cddp.dbxapi as dbxapi

# disable informational messages from prophet
import logging
logging.getLogger('py4j').setLevel(logging.ERROR)

if __name__ == "__main__":
    cddp.entrypoint()
```
2. Upload sample data and pipeline configuation file to the linked storage account (**/example/\*\***).
3. Open Synapse Studio, go to **develop**, and add a new Spark Job Definition.
4. Fill in the main definiation file path and command line arguments with the `abfss://` path of the main.py you uploaded in **step 1**.
![syn4.png](../images/syn4.png)
```
main definition file path: "abfss://[file system name]@[storage account name].dfs.core.windows.net/main.py"
command line arguments: "--config-path abfss://[file system name]@[storage account name].dfs.core.windows.net/example/pipeline_fruit_batch_ADLS.json --stage staging --task price_ingestion --working-dir ./tmp --show-result --build-landing-zone --cleanup-database"
```
Quickly find the `abfss://` path of your file using **data** tab in the Synapse Studio. Go to **data** --> **linked** --> **your storage account** --> find your file --> click **More** of top bar --> **Properties** --> copy the `abfss://` path.
5. Submit the job definition to start a run.
6. Publish the job.
6. You can also Create a pipeline to run multiple jobs.
![syn5.png](../images/syn5.png)

