import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import cddp
import streamlit as st
import pandas as pd
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import shutil
from delta import *
from delta.tables import *
import argparse
import time
import tempfile
import uuid
from io import StringIO
import numpy as np
from streamlit_echarts import st_echarts


# def build_pipeline_object():
#     pipeline_object = {
#         "name": st.session_state['pipeline_name'],
#         "description": st.session_state['pipeline_description'],
#         "industry": st.session_state['industry'],
#         "staging": [],
#         "standard": [],
#         "serving": []
#     }


#     for i in range( st.session_state['stg_count']):
#         spark = st.session_state["spark"]
#         if f'stg_{i}_data' in st.session_state:
            
#             json_str = st.session_state[f'stg_{i}_data']
#             schema = st.session_state[f'stg_{i}_schema']
            
#             if f'stg_{i}_name' in st.session_state and st.session_state[f'stg_{i}_name'] != "":
#                 task_name = st.session_state[f'stg_{i}_name']
#             else:
#                 task_name = st.session_state[f'stg_{i}_filename_without_ext']
           
#             stg_task = { 
#                 "name": task_name,
#                 "input": {
#                     "type": "filestore",
#                     "format": "csv",
#                     "path": f"/FileStore/cddp_apps/{st.session_state['pipeline_name']}/landing/{task_name}/",
#                     "read-type": "batch"
#                 },
#                 "output": {
#                     "target": task_name,
#                     "type": ["file", "view"]
#                 },
#                 "schema": json.loads(schema),
#                 "sampleData": json.loads(json_str)
#             }

#             pipeline_object["staging"].append(stg_task)

#     for i in range(st.session_state['std_count']):
#         if f'std_{i}_sql' in st.session_state and st.session_state[f'std_{i}_sql'] is not None and st.session_state[f'std_{i}_sql'] != "":
#             task_name = st.session_state[f'std_{i}_name']
#             sql = st.session_state[f'std_{i}_sql']
#             std_task = {
#                 "name": task_name,
#                 "type": "batch",
#                 "code": {
#                     "lang": "sql",
#                     "sql": [sql]
#                 },
#                 "output": {
#                     "target": task_name,
#                     "type": ["file", "view"]
#                 },
#                 "dependency":[]
#             }

#             pipeline_object["standard"].append(std_task)


#     for i in range(st.session_state['srv_count']):
#         if f'srv_{i}_sql' in st.session_state and st.session_state[f'srv_{i}_sql'] is not None and st.session_state[f'srv_{i}_sql'] != "":
#             task_name = st.session_state[f'srv_{i}_name']
#             sql = st.session_state[f'srv_{i}_sql']
#             srv_task =     {
#                 "name": task_name,
#                 "type": "batch",
#                 "code": {
#                     "lang": "sql",
#                     "sql": [sql]
#                 },
#                 "output": {
#                     "target": task_name,
#                     "type": ["file", "view"]
#                 },
#                 "dependency":[]
#             }

#             pipeline_object["serving"].append(srv_task)

#     return pipeline_object

def run_task(task_name, stage="standard"):
    dataframe = None
    try:
        spark = st.session_state["spark"]
        config = current_pipeline_obj
        with tempfile.TemporaryDirectory() as tmpdir:
            working_dir = tmpdir+"/"+config['name']
            cddp.init(spark, config, working_dir)
            cddp.clean_database(spark, config)
            cddp.init_database(spark, config)

        cddp.init_staging_sample_dataframe(spark, config)
        
        if stage in config:
            for task in config[stage]:
                
                if task_name == task['name']: 
                    print(f"start {stage} task: "+task_name)
                    res_df = None
                    if stage == "standard":
                        res_df = cddp.start_standard_job(spark, config, task, False, True)
                    elif stage == "serving":
                        res_df = cddp.start_serving_job(spark, config, task, False, True)
                    dataframe = res_df.toPandas()
                    print(dataframe)
                    st.session_state[f'_{task_name}_data'] = dataframe

    except Exception as e:
        print(f"Cannot run task: {e}")
        st.error(f"Cannot run task: {e}")

    return dataframe

def delete_task(type, index):
    if type == "staging":
        del current_pipeline_obj['staging'][index]
    elif type == "standard":
        del current_pipeline_obj['standard'][index]
    elif type == "serving":
        del current_pipeline_obj['serving'][index]
    elif type == "visualization":
        del current_pipeline_obj['visualization'][index]

    st.session_state['current_pipeline_obj'] = current_pipeline_obj 

def show_vis(vis_name, chart_type, serving_dataset_name):
    serving_dataset = run_task(serving_dataset_name, "serving")
    serving_dataset_colunm_values = []
    if serving_dataset is None:
        return None
    
    for i in list(serving_dataset):
        serving_dataset_colunm_values.append(serving_dataset[i].tolist())

    options = None
    if chart_type == "Bar Chart" or chart_type == "Line Chart":
        type = "bar"
        if chart_type == "Line Chart":
            type = "line"
        options = {
            "xAxis": {
                "type": "category",
                "data": serving_dataset_colunm_values[0],
            },
            "yAxis": {"type": "value"},
            "series": [{"data": serving_dataset_colunm_values[1], "type": type}],
        }
        print(options)
    st.session_state[f'_{vis_name}_chart_options'] = options
    
    return options
    

if "spark" not in st.session_state:
    spark = cddp.create_spark_session()
    st.session_state["spark"] = spark

if "current_pipeline_obj" not in st.session_state:

    st.session_state['current_pipeline_obj'] = {
        "name": "",
        "description": "",
        "industry": "Other",
        "staging": [],
        "standard": [],
        "serving": [],
        "visualization": []
    }



current_pipeline_obj = st.session_state['current_pipeline_obj']



st.header("Config-Driven Data Pipeline WebUI")
st.divider()
def import_pipeline():
    if 'imported_pipeline_file_flag' in st.session_state:
        del st.session_state['imported_pipeline_file_flag']

imported_pipeline_file = st.file_uploader(f'Import Pipeline', key=f'imported_pipeline_file', on_change=import_pipeline)


if 'imported_pipeline_file' in st.session_state and imported_pipeline_file and 'imported_pipeline_file_flag' not in st.session_state:
    print("imported_pipeline_file")
    imported_pipeline_json = StringIO(imported_pipeline_file.getvalue().decode("utf-8"))
    current_pipeline_obj = json.loads(imported_pipeline_json.read())
    st.session_state['current_pipeline_obj'] = current_pipeline_obj
    if 'visualization' not in current_pipeline_obj:
        current_pipeline_obj['visualization'] = []

    st.session_state['imported_pipeline_file_flag'] = True

st.subheader('General Information')
pipeline_name = st.text_input('Pipeline name', key='pipeline_name', value=current_pipeline_obj['name'])
if pipeline_name:
    current_pipeline_obj['name'] = pipeline_name
industry_list = ["Other", "Agriculture", "Automotive", "Banking", "Chemical", "Construction", "Education", "Energy", "Entertainment", "Food", "Government", "Healthcare", "Hospitality", "Insurance", "Machinery", "Manufacturing", "Media", "Mining", "Pharmaceutical", "Real Estate", "Retail", "Telecommunications", "Transportation", "Utilities", "Wholesale"]
industry_selected_idx = 0
if 'industry' in current_pipeline_obj:
    industry_selected_idx = industry_list.index(current_pipeline_obj['industry'])

pipeline_industry = st.selectbox('Industry', industry_list, key='industry', index=industry_selected_idx)
if pipeline_industry:
    current_pipeline_obj['industry'] = pipeline_industry


if 'description' not in current_pipeline_obj:
    current_pipeline_obj['description'] = ""

pipeline_desc = st.text_area('Pipeline description', key='pipeline_description', value=current_pipeline_obj['description'])
if pipeline_desc:
    current_pipeline_obj['description'] = pipeline_desc

st.divider()

wizard_view, code_view, deployment_view = st.tabs(["Wizard", "JSON", "Deployment"])

btn_settings_editor_btns = [{
    "name": "update",
    "feather": "RefreshCw",
    "primary": True,
    "hasText": True,
    "showWithIcon": True,
    "commands": ["submit"],
    "style": {"bottom": "0rem", "right": "0.4rem"}
  }]

with wizard_view:
    st.subheader('Staging Zone')



    pipeline_obj = st.session_state['current_pipeline_obj']
    


    
    for i in range(len(pipeline_obj["staging"]) ):
        
        target_name = pipeline_obj['staging'][i]['output']['target']
        stg_name = st.text_input(f'Dataset Name', key=f"stg_{i}_name", value=target_name)
        if stg_name:
            with st.expander(stg_name+" Settings"):
                st.selectbox(
                'Choose a dataset to add to staging zone',
                    ['Dataset 1', 'Dataset 2', 'Dataset 3', 'Dataset 4', 'Dataset 5'],  key=f'stg_{i}_ai_dataset')
                
                
                pipeline_obj['staging'][i]['output']['target'] = stg_name
                pipeline_obj['staging'][i]['name'] = stg_name

                if 'description' not in pipeline_obj['staging'][i]:
                    pipeline_obj['staging'][i]['description'] = ""
                stg_desc = st.text_area(f'Dataset Description', key=f"stg_{i}desc", value=pipeline_obj['staging'][i]['description'])
                if stg_desc:
                    pipeline_obj['staging'][i]['description'] = stg_desc

                uploaded_file = st.file_uploader(f'Choose sample csv file', key=f'stg_{i}_file')
                if uploaded_file is not None:

                    
                    dataframe = pd.read_csv(uploaded_file)
                    spark = st.session_state["spark"]
                    stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
                    data_str = stringio.read()
                    json_str, schema = cddp.load_sample_data(spark, data_str, format="csv")
                    pipeline_obj['staging'][i]['sampleData'] = json.loads(json_str)
                    pipeline_obj['staging'][i]['schema'] = json.loads(schema)


                if 'sampleData' in pipeline_obj['staging'][i] and len(pipeline_obj['staging'][i]['sampleData']) > 0:
                    sampleData = pipeline_obj['staging'][i]['sampleData']
                    dataframe = pd.DataFrame(sampleData)
                    st.dataframe(dataframe)   

                st.button("Delete", key="delete_stg_"+str(i), on_click=delete_task, args = ['staging', i])

        if i != len(pipeline_obj["staging"]) - 1:
            st.divider()

        

    def add_dataset():

        pipeline_obj = st.session_state['current_pipeline_obj']
        task_name = "untitled"+str(len(pipeline_obj["staging"])+1)


     
        pipeline_obj["staging"].append({
            "name": task_name,
            "description": "",
            "input": {
                "type": "filestore",
                "format": "csv",
                "path": "",
                "read-type": "batch"
            },
            "output": {
                "target": task_name,
                "type": ["file", "view"]
            },
            "schema": {},
            "sampleData": []
        })
       

    st.button('Add Dataset', on_click=add_dataset)





    st.subheader('Standardization Zone')

    for i in range(len(pipeline_obj["standard"])):
        target_name = pipeline_obj['standard'][i]['output']['target']
        std_name = st.text_input(f'Transformation Name', key=f'std_{i}_name', value=target_name)
        if std_name:
            with st.expander(std_name+" Settings"):
        
                pipeline_obj['standard'][i]['output']['target'] = std_name
                pipeline_obj['standard'][i]['name'] = std_name

                if 'description' not in pipeline_obj['standard'][i]:
                    pipeline_obj['standard'][i]['description'] = ""

                std_desc = st.text_area(f'Transformation Description', key=f'std_{i}_description', value=pipeline_obj['standard'][i]['description'])
                if std_desc:
                    pipeline_obj['standard'][i]['description'] = std_desc

                st.multiselect(
                'Choose datasets to add to transformation',
                    ['Dataset 1', 'Dataset 2', 'Dataset 3', 'Dataset 4', 'Dataset 5'],  key=f'std_{i}_ai_dataset')
                
                st.button(f'Generate SQL', key=f'std_{i}_gen')
                if len(current_pipeline_obj['standard'][i]['code']['sql']) == 0:
                    current_pipeline_obj['standard'][i]['code']['sql'].append("")
                std_sql_val = current_pipeline_obj['standard'][i]['code']['sql'][0]
                std_sql = st.text_area(f'SQL', key=f'std_{i}_sql', value=std_sql_val)   
                if std_sql:
                    pipeline_obj['standard'][i]['code']['sql'][0] = std_sql


                st.button(f'Run SQL', key=f'run_std_{i}_sql', on_click=run_task, args = [std_name, "standard"])
                if '_'+std_name+'_data' in st.session_state:
                    st.dataframe(st.session_state['_'+std_name+'_data'])

                st.button("Delete", key="delete_std_"+str(i), on_click=delete_task, args = ['standard', i])

        if i != len(pipeline_obj["standard"]) - 1:
            st.divider()

    def add_transformation():
        task_name = "untitled"+str(len(current_pipeline_obj["standard"])+1)
        current_pipeline_obj["standard"].append({
            "name": task_name,
            "type": "batch",
            "code": {
                "lang": "sql",
                "sql": []
            },
            "output": {
                "target": task_name,
                "type": ["file", "view"]
            },
            "dependency":[]
        })
        
    st.button('Add Transformation', on_click=add_transformation)
    st.divider()






    st.subheader('Serving Zone')

    for i in range(len(pipeline_obj["serving"])):
        target_name = pipeline_obj['serving'][i]['output']['target']
        srv_name = st.text_input(f'Aggregation Name', key=f'srv_{i}_name', value=target_name)

        if srv_name:
            with st.expander(srv_name+" Settings"):
                pipeline_obj['serving'][i]['output']['target'] = srv_name
                pipeline_obj['serving'][i]['name'] = srv_name

                if 'description' not in pipeline_obj['serving'][i]:
                    pipeline_obj['serving'][i]['description'] = ""

                srv_desc = st.text_area(f'Aggregation Description', key=f'srv_{i}_description', value=pipeline_obj['serving'][i]['description'])
                if srv_desc:
                    pipeline_obj['serving'][i]['description'] = srv_desc

                st.multiselect(
                'Choose datasets to add to aggregation',
                    ['Dataset 1', 'Dataset 2', 'Dataset 3', 'Dataset 4', 'Dataset 5'],  key=f'srv_{i}_ai_dataset')
                
                st.button(f'Generate SQL', key=f'srv_{i}_gen')
                if len(current_pipeline_obj['serving'][i]['code']['sql']) == 0:
                    current_pipeline_obj['serving'][i]['code']['sql'].append("")
                srv_sql_val = current_pipeline_obj['serving'][i]['code']['sql'][0]
                srv_sql = st.text_area(f'SQL', key=f'srv_{i}_sql', value=srv_sql_val)   
                if srv_sql:
                    pipeline_obj['serving'][i]['code']['sql'][0] = srv_sql


                st.button(f'Run SQL', key=f'run_srv_{i}_sql', on_click=run_task, args = [srv_name, "serving"])
                if f'_{srv_name}_data' in st.session_state:
                    st.dataframe(st.session_state[f'_{srv_name}_data'])

                st.button("Delete", key="delete_srv_"+str(i), on_click=delete_task, args = ['serving', i])

        if i != len(pipeline_obj["serving"]) - 1:
            st.divider()
    
    def add_aggregation():
        task_name = "untitled"+str(len(current_pipeline_obj["serving"])+1)
        current_pipeline_obj["serving"].append({
            "name": task_name,
            "type": "batch",
            "code": {
                "lang": "sql",
                "sql": []
            },
            "output": {
                "target": task_name,
                "type": ["file", "view"]
            },
            "dependency":[]
        })

    st.button('Add Aggregation', on_click=add_aggregation)
    st.divider()

    st.subheader('Visualization')

    for i in range(len(pipeline_obj["visualization"])):
        target_name = pipeline_obj['visualization'][i]['name']
        vis_name = st.text_input(f'Visualization Name', key=f'vis_{i}_name', value=target_name)
        if vis_name:
            with st.expander(vis_name+" Settings"):
                pipeline_obj['visualization'][i]['name'] = vis_name

                if 'description' not in pipeline_obj['visualization'][i]:
                    pipeline_obj['visualization'][i]['description'] = ""

                vis_desc = st.text_area(f'Visualization Description', key=f'vis_{i}_description', value=pipeline_obj['visualization'][i]['description'])
                if vis_desc:
                    pipeline_obj['visualization'][i]['description'] = vis_desc

                serving_datasets = []
                for serving_tasks in pipeline_obj["serving"]:
                    serving_datasets.append(serving_tasks['name'])
                vis_dataset = st.selectbox("Serving Dataset", serving_datasets, key=f'vis_{i}_dataset')
                if vis_dataset:
                    pipeline_obj['visualization'][i]['input'] = vis_dataset
                chart_type = st.selectbox("Chart Type", ["Bar Chart", "Line Chart", "Pie Chart", "Scatter Chart"], key=f'vis_{i}_chart_type')
                if chart_type:
                    pipeline_obj['visualization'][i]['type'] = chart_type
                    
                st.button(f'Run Visualization', key=f'run_vis_{i}', on_click=show_vis, args = [vis_name, chart_type, pipeline_obj['visualization'][i]['input']])
                if f'_{vis_name}_chart_options' in st.session_state:
                    options = st.session_state[f'_{vis_name}_chart_options']
                    if options is not None:
                        st_echarts(options=options, height="500px")
                
                st.button("Delete", key="delete_vis_"+str(i), on_click=delete_task, args = ['visualization', i])
            

        if i != len(pipeline_obj["visualization"]) - 1:
            st.divider()

    def add_visualization():
        task_name = "untitled"+str(len(current_pipeline_obj["visualization"])+1)
        current_pipeline_obj["visualization"].append({
            "name": task_name,
            "type": "Bar Chart",
            "input": "",
        })


    st.button('Add Visualization', on_click=add_visualization)
    if len(pipeline_obj["visualization"]) == 0:
        st.divider()




with code_view:

    pipeline_json = json.dumps(current_pipeline_obj, indent=4)
    code = pipeline_json
    st.code(code, language='json')

with deployment_view:
    st.write('Deployment')
