import datetime
import os, uuid
from dotenv import load_dotenv
load_dotenv() 
import os, sys, json
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from azure.data.tables import TableServiceClient


PIPELINE_TABLE_NAME = "pipelines"
PIPELINE_INDEX_TABLE_NAME = "pipelineindex"
STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")

def insert_new_pipeline_entity(account_id, pipeline, conn_str=None):
    if conn_str is None:
        conn_str = os.getenv(STORAGE_CONNECTION_STRING)

    publish_date = datetime.datetime.now()
    pipeline["publish_date"] = publish_date.strftime("%Y-%m-%d %H:%M:%S")
    
    table_service = TableServiceClient.from_connection_string(conn_str)
    
    idx_table_client = table_service.get_table_client(PIPELINE_INDEX_TABLE_NAME)
    pipeline_idx_params = pipeline_idx_deserialize(account_id, pipeline)

    idx_table_client.upsert_entity(pipeline_idx_params)

    table_client = table_service.get_table_client(PIPELINE_TABLE_NAME)
    pipeline_params = pipeline_deserialize(account_id, pipeline)

    table_client.upsert_entity(pipeline_params)



def pipeline_deserialize(account_id, pipeline):
    params = {}
    params["PartitionKey"] = pipeline["id"]
    params["RowKey"] = pipeline["id"]
    params["account_id"] = account_id
    params["body"] = json.dumps(pipeline)
    return params

def pipeline_idx_deserialize(account_id, pipeline):

    params = {}
    params["PartitionKey"] = pipeline["id"]
    params["RowKey"] = pipeline["id"]
    params["name"] = pipeline["name"]
    params["description"] = pipeline["description"]
    params["publish_date"] = pipeline["publish_date"]
    params['industry'] = pipeline['industry']
    params["account_id"] = account_id
    return params


def load_all_pipelines(conn_str=None):
    if conn_str is None:
        conn_str = os.getenv(STORAGE_CONNECTION_STRING)
    table_service = TableServiceClient.from_connection_string(conn_str)
    table_client = table_service.get_table_client(PIPELINE_INDEX_TABLE_NAME)
    pipelines = []
    for entity in table_client.list_entities():
        pipelines.append(entity)
    return pipelines

def load_pipeline_by_id(pipeline_id, conn_str=None):
    if conn_str is None:
        conn_str = os.getenv(STORAGE_CONNECTION_STRING)
    table_service = TableServiceClient.from_connection_string(conn_str)
    table_client = table_service.get_table_client(PIPELINE_TABLE_NAME)
    pipeline = table_client.get_entity(pipeline_id, pipeline_id)
    return json.loads(pipeline["body"])
