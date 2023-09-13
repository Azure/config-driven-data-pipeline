import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
import cddp
import cddp.dbxapi as dbxapi
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
from streamlit_extras.chart_container import chart_container
from streamlit_extras.stylable_container import stylable_container
from streamlit_extras.colored_header import colored_header
from streamlit_extras.grid import grid
import utils.gallery_storage as gallery_storage
import utils.ui_utils as ui_utils
from streamlit_extras.switch_page_button import switch_page
import streamlit_utils
import string

if "working_folder" not in st.session_state:
    switch_page("Home")

if "current_pipeline_obj" not in st.session_state:
    switch_page("Home")

st.set_page_config(page_title="CDDP - Pipeline Editor")

current_pipeline_obj = st.session_state['current_pipeline_obj']



chart_types = ['Bar Chart', 'Line Chart', 'Area Chart', 'Scatter Chart', 'Pie Chart' ]
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
        
    st.session_state[f'_{vis_name}_chart_options'] = options
    
    return options


def contains_whitespace(s):
    for c in s:
        if c==' ':
            return True
    return False

def validate_name():
    validation_res = []
    name = current_pipeline_obj['name']
    if contains_whitespace(name):
        validation_res.append("Pipeline ID cannot contain whitespace")
        
    if 'staging' in current_pipeline_obj:
        for task in current_pipeline_obj["staging"]:        
            if contains_whitespace(task['name']):
                validation_res.append("Dataset name cannot contain whitespace")
                
    if 'standard' in current_pipeline_obj:
        for task in current_pipeline_obj["standard"]:
            if contains_whitespace(task['name']):
                validation_res.append("Transformation name cannot contain whitespace")
                
    if 'serving' in current_pipeline_obj :
        for task in current_pipeline_obj["serving"]:
            if contains_whitespace(task['name']):
                validation_res.append("Aggregation name cannot contain whitespace")
                
    if 'visualization' in current_pipeline_obj:
        for task in current_pipeline_obj["visualization"]:
            if contains_whitespace(task['name']):
                validation_res.append("Visualization name cannot contain whitespace")
                
    return validation_res

validation_res = validate_name()

if len(validation_res) > 0:
    for i in range(len(validation_res)):
        st.error(validation_res[i])
    

def build_pipeline_preview():
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
        preview_obj = {
            "staging": {},
            "standard": {},
            "serving": {},
            "visualization": {}
        }
        if 'staging' in config:
            for task in config["staging"]:        
                preview_obj["staging"][task['name']] = task["sampleData"]
        if 'standard' in config:
            for task in config["standard"]:
                df = cddp.start_standard_job(spark, config, task, False, True)
                preview_obj["standard"][task['name']] = df.toJSON().map(lambda j: json.loads(j)).collect()
        if 'serving' in config :
            for task in config["serving"]:
                df = cddp.start_serving_job(spark, config, task, False, True)
                preview_obj["serving"][task['name']] = df.toJSON().map(lambda j: json.loads(j)).collect()
        if 'visualization' in config:
            for task in config["visualization"]:
                preview_obj["visualization"][task['name']] = task

        current_pipeline_obj['preview'] = preview_obj
        return preview_obj
                
    except Exception as e:
        st.error(f"Cannot run pipeline: {e}")

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
        try:
            cddp.init_staging_sample_dataframe(spark, config)
        except Exception as e:
            print(e)
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





if "spark" not in st.session_state:
    with st.spinner('Loading Spark session...'):
        spark = cddp.create_spark_session()
        st.session_state["spark"] = spark




colored_header(
    label="Config-Driven Data Pipeline Editor",
    description=f"Simplification of data pipeline development",
    color_name="violet-70",
)




def import_pipeline():
    if 'imported_pipeline_file_flag' in st.session_state:
        del st.session_state['imported_pipeline_file_flag']

def get_pipeline_path():
    if 'working_folder' not in st.session_state:
        return"./"+current_pipeline_obj['id']+".json"
    else:        
        return st.session_state["working_folder"]+"/"+current_pipeline_obj['id']+".json"

def save_pipeline_to_workspace():
    pipeline_path = get_pipeline_path()
    pipeline_json = json.dumps(current_pipeline_obj)
    pipeline_json_file = open(pipeline_path, "w")
    pipeline_json_file.write(pipeline_json)
    pipeline_json_file.close()

def delete_pipeline_from_workspace():
    pipeline_path = get_pipeline_path()
    if os.path.exists(pipeline_path):
        os.remove(pipeline_path)

def publish_pipeline_to_gallery():
    settings_path = os.path.join(st.session_state["working_folder"], ".settings.json")
    account_id = None
    gallery_token = None
    if os.path.exists(settings_path):
        with open(settings_path, "r") as f:
            settings_obj = json.load(f)
            account_id = settings_obj["account_id"]
            gallery_token = settings_obj["gallery_token"]
    if account_id is None:
        st.error("Please set account ID in settings.")
        return
    if gallery_token is None:
        st.error("Please set gallery token in settings.")
        return

    
    publish_project_obj = {}
    publish_project_obj['name'] = current_pipeline_obj['name']
    publish_project_obj['description'] = current_pipeline_obj['description']
    publish_project_obj['industry'] = current_pipeline_obj['industry']
    publish_project_obj['staging'] = current_pipeline_obj['staging']
    publish_project_obj['standard'] = current_pipeline_obj['standard']
    publish_project_obj['serving'] = current_pipeline_obj['serving']
    publish_project_obj['visualization'] = current_pipeline_obj['visualization']
    publish_project_obj['id'] = current_pipeline_obj['id']
    preview_obj = build_pipeline_preview()
    publish_project_obj['preview'] = preview_obj
    # st.code(json.dumps(publish_project_obj, indent=4))
    gallery_storage.insert_new_pipeline_entity(account_id, publish_project_obj, gallery_token)

def deploy_pipeline():
    working_dir = "/FileStore/cddp_apps/" + current_pipeline_obj['name'] + "/"
    resp = dbxapi.deploy_pipeline(current_pipeline_obj, current_pipeline_obj['name'], working_dir)
    st.session_state['deployed_pipeline_id'] = resp['job_id']
    return resp['job_id']

btn_grid = grid(5, 1)

btn_grid.button('New', on_click=streamlit_utils.create_pipeline, use_container_width=True)
pipeline_saved = btn_grid.button('Save', on_click=save_pipeline_to_workspace,  use_container_width=True)
pipeline_published = btn_grid.button('Publish', on_click=publish_pipeline_to_gallery,  use_container_width=True)
pipeline_deployed = btn_grid.button('Deploy', on_click=deploy_pipeline,  use_container_width=True)
btn_grid.button('Delete', on_click=delete_pipeline_from_workspace,  use_container_width=True)

# pipeline_saved = st.button('Save Pipeline to Workspace', key='save_pipeline', on_click=save_pipeline_to_workspace)

if pipeline_saved:
    st.write("Saved as "+get_pipeline_path())
if pipeline_published:
    st.write("Published to gallery")
if pipeline_deployed:
    st.write("Deployed to cloud, job id: "+str(+st.session_state['deployed_pipeline_id']))


# st.divider()

with btn_grid.expander("Import Pipeline"):
    imported_pipeline_file = st.file_uploader(f'Choose a pipeline JSON file', key=f'imported_pipeline_file', on_change=import_pipeline)


    if 'imported_pipeline_file' in st.session_state and imported_pipeline_file and 'imported_pipeline_file_flag' not in st.session_state:
        imported_pipeline_json = StringIO(imported_pipeline_file.getvalue().decode("utf-8"))
        current_pipeline_obj = json.loads(imported_pipeline_json.read())
        st.session_state['current_pipeline_obj'] = current_pipeline_obj
        if 'visualization' not in current_pipeline_obj:
            current_pipeline_obj['visualization'] = []
        if 'id' not in current_pipeline_obj:
            current_pipeline_obj['id'] = str(uuid.uuid4())

        st.session_state['imported_pipeline_file_flag'] = True


wizard_view, preview_view = st.tabs(["Editor", "Preview"])

with wizard_view:

    general_view, stg_view, std_view, srv_view, vis_view, code_view = st.tabs(["General", "Staging", "Standardization", "Serving", "Visualization", "JSON View"])

    # st.subheader('General Information')
    with general_view:
        pipeline_name = st.text_input('Pipeline name', key='pipeline_name', value=current_pipeline_obj['name'], on_change=validate_name)
        if pipeline_name:
            current_pipeline_obj['name'] = pipeline_name
        st.text_input('Pipeline ID', key='pipeline_id', value=current_pipeline_obj['id'], disabled=True)
        industry_list = ["Other", "Airlines", "Agriculture", "Automotive", "Banking", "Chemical", "Construction", "Education", "Energy", "Entertainment", "Food", "Government", "Healthcare", "Hospitality", "Insurance", "Machinery", "Manufacturing", "Media", "Mining", "Pharmaceutical", "Real Estate", "Retail", "Telecommunications", "Transportation", "Utilities", "Wholesale"]
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



    # st.divider()

    # st.subheader('Staging Zone')

    with stg_view:

        pipeline_obj = st.session_state['current_pipeline_obj']
        
        for i in range(len(pipeline_obj["staging"]) ):
            
            target_name = pipeline_obj['staging'][i]['output']['target']
            stg_name = st.text_input(f'Dataset Name', key=f"stg_{i}_name", value=target_name)
            if stg_name:
                st.subheader(stg_name)
                with st.expander(stg_name+" Settings"):
                    # st.selectbox(
                    # 'Choose a dataset to add to staging zone',
                    #     ['Dataset 1', 'Dataset 2', 'Dataset 3', 'Dataset 4', 'Dataset 5'],  key=f'stg_{i}_ai_dataset')
                                    
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

                st.button(f"Delete {stg_name}", key="delete_stg_"+str(i), on_click=delete_task, args = ['staging', i])

            if i != len(pipeline_obj["staging"]) - 1:
                st.divider()

        if len(pipeline_obj["staging"]) == 0:
            st.write("No dataset in staging zone")
               

        def add_dataset():

            pipeline_obj = st.session_state['current_pipeline_obj']
            task_name = "untitled"+str(len(pipeline_obj["staging"])+1)     
            streamlit_utils.add_stg_dataset(pipeline_obj, task_name)
        
        st.divider() 
        st.button('Add Dataset', on_click=add_dataset, use_container_width=True)


    # st.divider()


    # st.subheader('Standardization Zone')


    with std_view:

        for i in range(len(pipeline_obj["standard"])):
            target_name = pipeline_obj['standard'][i]['output']['target']
            std_name = st.text_input(f'Transformation Name', key=f'std_{i}_name', value=target_name)
            if std_name:
                st.subheader(std_name)
                with st.expander(std_name+" Settings"):
            
                    pipeline_obj['standard'][i]['output']['target'] = std_name
                    pipeline_obj['standard'][i]['name'] = std_name

                    if 'description' not in pipeline_obj['standard'][i]:
                        pipeline_obj['standard'][i]['description'] = ""

                    std_desc = st.text_area(f'Transformation Description', key=f'std_{i}_description', value=pipeline_obj['standard'][i]['description'])
                    if std_desc:
                        pipeline_obj['standard'][i]['description'] = std_desc

                    # st.multiselect(
                    # 'Choose datasets to add to transformation',
                    #     ['Dataset 1', 'Dataset 2', 'Dataset 3', 'Dataset 4', 'Dataset 5'],  key=f'std_{i}_ai_dataset')
                    
                    # st.button(f'Generate SQL', key=f'std_{i}_gen')
                    if len(current_pipeline_obj['standard'][i]['code']['sql']) == 0:
                        current_pipeline_obj['standard'][i]['code']['sql'].append("")
                    std_sql_val = current_pipeline_obj['standard'][i]['code']['sql'][0]
                    std_sql = st.text_area(f'SQL', key=f'std_{i}_sql', value=std_sql_val)   
                    if std_sql:
                        pipeline_obj['standard'][i]['code']['sql'][0] = std_sql


                    st.button(f'Run SQL', key=f'run_std_{i}_sql', on_click=run_task, args = [std_name, "standard"])
                    if '_'+std_name+'_data' in st.session_state:
                        st.dataframe(st.session_state['_'+std_name+'_data'])

                st.button(f"Delete {std_name}", key="delete_std_"+str(i), on_click=delete_task, args = ['standard', i])

            if i != len(pipeline_obj["standard"]) - 1:
                st.divider()

        if len(pipeline_obj["standard"]) == 0:
            st.write("No transformation in standardization zone")
            

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
        st.divider()
        st.button('Add Transformation', on_click=add_transformation, use_container_width=True)
    # st.divider()

    # st.subheader('Serving Zone')

    with srv_view:

        for i in range(len(pipeline_obj["serving"])):
            target_name = pipeline_obj['serving'][i]['output']['target']
            srv_name = st.text_input(f'Aggregation Name', key=f'srv_{i}_name', value=target_name)

            if srv_name:
                st.subheader(srv_name)
                with st.expander(srv_name+" Settings"):
                    pipeline_obj['serving'][i]['output']['target'] = srv_name
                    pipeline_obj['serving'][i]['name'] = srv_name

                    if 'description' not in pipeline_obj['serving'][i]:
                        pipeline_obj['serving'][i]['description'] = ""

                    srv_desc = st.text_area(f'Aggregation Description', key=f'srv_{i}_description', value=pipeline_obj['serving'][i]['description'])
                    if srv_desc:
                        pipeline_obj['serving'][i]['description'] = srv_desc

                    # st.multiselect(
                    # 'Choose datasets to add to aggregation',
                    #     ['Dataset 1', 'Dataset 2', 'Dataset 3', 'Dataset 4', 'Dataset 5'],  key=f'srv_{i}_ai_dataset')
                    
                    st.button(f'Generate SQL', key=f'srv_{i}_gen')
                    if len(current_pipeline_obj['serving'][i]['code']['sql']) == 0:
                        current_pipeline_obj['serving'][i]['code']['sql'].append("")
                    srv_sql_val = current_pipeline_obj['serving'][i]['code']['sql'][0]
                    srv_sql = st.text_area(f'SQL', key=f'srv_{i}_sql', value=srv_sql_val)   
                    if srv_sql:
                        pipeline_obj['serving'][i]['code']['sql'][0] = srv_sql


                    st.button(f'Run SQL', key=f'run_srv_{i}_sql', on_click=run_task, args = [srv_name, "serving"])

                    if f'_{srv_name}_data' in st.session_state:
                        chart_data = st.session_state[f'_{srv_name}_data']
                        # chart_type = st.selectbox('Chart Type', chart_types, key=f'chart_type_{i}')
                        # cols = chart_data.columns.values.tolist()
                        # if chart_type == 'Bar Chart' or chart_type == 'Line Chart' or chart_type == 'Area Chart' or chart_type == 'Scatter Chart':
                        #     x_axis = st.selectbox('X Axis', cols, key=f'x_axis_{i}', index=0)
                        #     y_axis = st.selectbox('Y Axis', cols,key=f'y_axis_{i}', index=1 if len(cols) > 1 else 0)
                        #     if chart_type == 'Scatter Chart':
                        #        scatter_size = st.selectbox('Size', cols, key=f'size_{i}', index=2 if len(cols) > 2 else 0)
                        # elif chart_type == 'Pie Chart':
                        #     cate_axis = st.selectbox('Category', cols, key=f'cate_axis_{i}', index=0)
                        #     val_axis = st.selectbox('Value', cols,key=f'val_axis_{i}', index=1 if len(cols) > 1 else 0)
                            
                        chart_data = (st.session_state[f'_{srv_name}_data'])
                        st.dataframe(chart_data)
                        # task_name = srv_name+"_chart_"+str(len(current_pipeline_obj["visualization"])+1)
                        
                        # with chart_container(chart_data):
                            
                        #     if chart_type == 'Bar Chart':
                        #         st.bar_chart(chart_data, x=st.session_state[f'x_axis_{i}'], y=st.session_state[f'y_axis_{i}'])
                        #         def add_bar_vis():
                        #             st.session_state[f'vis_{task_name}_data'] = chart_data
                        #             current_pipeline_obj["visualization"].append({
                        #                 "name": task_name,
                        #                 "type": chart_type,
                        #                 "serving_data": srv_name,
                        #                 "x_axis": x_axis,
                        #                 "y_axis": y_axis,
                        #                 "description": "",
                        #             })
                        #         st.button("Add As Visualization", key="add_as_vis_"+str(i), on_click=add_bar_vis)
                        #     elif chart_type == 'Line Chart':
                        #         st.line_chart(chart_data, x=st.session_state[f'x_axis_{i}'], y=st.session_state[f'y_axis_{i}'])
                        #         def add_line_vis():
                        #             st.session_state[f'vis_{task_name}_data'] = chart_data
                        #             current_pipeline_obj["visualization"].append({
                        #                 "name": task_name,
                        #                 "type": chart_type,
                        #                 "serving_data": srv_name,
                        #                 "x_axis": x_axis,
                        #                 "y_axis": y_axis,
                        #                 "description": "",
                        #             })
                        #         st.button("Add As Visualization", key="add_as_vis_"+str(i), on_click=add_line_vis)
                        #     elif chart_type == 'Area Chart':
                        #         st.area_chart(chart_data,  x=st.session_state[f'x_axis_{i}'], y=st.session_state[f'y_axis_{i}'])
                        #         def add_area_vis():
                        #             st.session_state[f'vis_{task_name}_data'] = chart_data
                        #             current_pipeline_obj["visualization"].append({
                        #                 "name": task_name,
                        #                 "type": chart_type,
                        #                 "serving_data": srv_name,
                        #                 "x_axis": x_axis,
                        #                 "y_axis": y_axis,
                        #                 "description": "",
                        #             })
                        #         st.button("Add As Visualization", key="add_as_vis_"+str(i), on_click=add_area_vis)
                        #     elif chart_type == 'Scatter Chart':
                        #         st.vega_lite_chart(chart_data, {
                        #             'mark': {'type': 'circle', 'tooltip': True},
                        #             'encoding': {
                        #                 'x': {'field': x_axis, "type": "nominal"},
                        #                 'y': {'field': y_axis, 'type': 'quantitative'},
                        #                 'size': {'field': scatter_size, 'type': 'quantitative'},
                        #                 'color': {'field': scatter_size, 'type': 'quantitative'},
                        #             },
                        #         },use_container_width=True)
                        #         def add_scatter_vis():
                        #             st.session_state[f'vis_{task_name}_data'] = chart_data
                        #             current_pipeline_obj["visualization"].append({
                        #                 "name": task_name,
                        #                 "type": chart_type,
                        #                 "serving_data": srv_name,
                        #                 "x_axis": x_axis,
                        #                 "y_axis": y_axis,
                        #                 "scatter_size": scatter_size,
                        #                 "description": "",
                        #             })
                        #         st.button("Add As Visualization", key="add_as_vis_"+str(i), on_click=add_scatter_vis)

                        #     elif chart_type == 'Pie Chart':
                        #         st.vega_lite_chart(chart_data, {
                        #             "mark": {"type": "arc", "innerRadius": 70},
                        #             "encoding": {
                        #                 "theta": {"field": val_axis, "type": "quantitative"},
                        #                 "color": {"field": cate_axis, "type": "nominal"}
                        #             }
                        #         },use_container_width=True)
                        #         def add_pie_vis():
                        #             st.session_state[f'vis_{task_name}_data'] = chart_data
                        #             current_pipeline_obj["visualization"].append({
                        #                 "name": task_name,
                        #                 "type": chart_type,
                        #                 "serving_data": srv_name,
                        #                 "val_axis": val_axis,
                        #                 "cate_axis": cate_axis,
                        #                 "description": "",
                        #             })
                        #         st.button("Add As Visualization", key="add_as_vis_"+str(i), on_click=add_pie_vis)
                    st.divider()

               
                st.button(f"Delete {srv_name}", key="delete_srv_"+str(i), on_click=delete_task, args = ['serving', i])

            if i != len(pipeline_obj["serving"]) - 1:
                st.divider()
        
        if len(pipeline_obj["serving"]) == 0:
            st.write("No Aggregation")
            

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
        st.divider()
        st.button('Add Aggregation', on_click=add_aggregation, use_container_width=True)

    with vis_view:
    # st.subheader('Chart')

        def clean_vis_data(vis_name):
            if f'vis_{vis_name}_data' in st.session_state:
                del st.session_state[f'vis_{vis_name}_data']

        for i in range(len(pipeline_obj["visualization"])):
            target_name = pipeline_obj['visualization'][i]['name']
            vis_name = st.text_input(f'Chart Name', key=f'vis_{i}_name', value=target_name)
            if vis_name:
                st.subheader(vis_name)
                with st.expander(vis_name+" Settings"):
                    pipeline_obj['visualization'][i]['name'] = vis_name

                    if 'description' not in pipeline_obj['visualization'][i]:
                        pipeline_obj['visualization'][i]['description'] = ""

                    vis_desc = st.text_area(f'Chart Description', key=f'vis_{i}_description', value=pipeline_obj['visualization'][i]['description'])
                    if vis_desc:
                        pipeline_obj['visualization'][i]['description'] = vis_desc

                    serving_datasets = []
                    for serving_tasks in pipeline_obj["serving"]:
                        serving_datasets.append(serving_tasks['name'])

                    vis_dataset = st.selectbox("Serving Dataset", serving_datasets, key=f'vis_{i}_dataset', on_change=clean_vis_data, args = [vis_name])
                    if vis_dataset:
                        pipeline_obj['visualization'][i]['input'] = vis_dataset
                        fetch_data = st.button(f'Fetch Data', key=f'fetch_data_vis_{i}')
                        if fetch_data:
                            chart_data = run_task(vis_dataset, "serving")
                            st.session_state[f'vis_{vis_name}_data'] = chart_data

                        if f'vis_{vis_name}_data' in st.session_state:
                            chart_data = st.session_state[f'vis_{vis_name}_data']
                            selected_chart_type_index = 0
                            for j in range(len(chart_types)):
                                if chart_types[j] == pipeline_obj['visualization'][i]['type']:
                                    selected_chart_type_index = j
                            chart_type = st.selectbox("Chart Type", chart_types, key=f'vis_{i}_chart_type', index=selected_chart_type_index)
                            if chart_type:
                                pipeline_obj['visualization'][i]['type'] = chart_type
                                cols = chart_data.columns.values.tolist()
                                if chart_type == 'Bar Chart' or chart_type == 'Line Chart' or chart_type == 'Area Chart' or chart_type == 'Scatter Chart':
                                    
                                    x_axis = st.selectbox('X Axis', cols, key=f'vis_x_axis_{i}', index=0)
                                    y_axis = st.selectbox('Y Axis', cols, key=f'vis_y_axis_{i}', index=1 if len(cols) > 1 else 0)
                                    if x_axis:
                                        pipeline_obj['visualization'][i]['x_axis'] = x_axis
                                    if y_axis:
                                        pipeline_obj['visualization'][i]['y_axis'] = y_axis
                                        
                                    if chart_type == 'Scatter Chart':
                                        scatter_size = st.selectbox('Size', cols, key=f'vis_size_{i}', index=2 if len(cols) > 2 else 0)
                                        if scatter_size:
                                            pipeline_obj['visualization'][i]['scatter_size'] = scatter_size

                                elif chart_type == 'Pie Chart':
                                    cate_axis = st.selectbox('Category', cols, key=f'vis_cate_axis_{i}', index=0)
                                    if cate_axis:
                                        pipeline_obj['visualization'][i]['cate_axis'] = cate_axis
                                    val_axis = st.selectbox('Value', cols,key=f'vis_val_axis_{i}', index=1 if len(cols) > 1 else 0)
                                    if val_axis:
                                        pipeline_obj['visualization'][i]['val_axis'] = val_axis


                                if chart_type == 'Bar Chart':
                                    st.bar_chart(chart_data, x=x_axis, y=y_axis)
                                elif chart_type == 'Line Chart':
                                    st.line_chart(chart_data, x=x_axis, y=y_axis)                    
                                elif chart_type == 'Area Chart':
                                    st.area_chart(chart_data,  x=x_axis, y=y_axis)                    
                                elif chart_type == 'Scatter Chart':
                                    st.vega_lite_chart(chart_data, {
                                        'mark': {'type': 'circle', 'tooltip': True},
                                        'encoding': {
                                            'x': {'field': x_axis, "type": "nominal"},
                                            'y': {'field': y_axis, 'type': 'quantitative'},
                                            'size': {'field': scatter_size, 'type': 'quantitative'},
                                            'color': {'field': scatter_size, 'type': 'quantitative'},
                                        },
                                    },use_container_width=True)
                                elif chart_type == 'Pie Chart':
                                    st.vega_lite_chart(chart_data, {
                                        "mark": {"type": "arc", "innerRadius": 70},
                                        "encoding": {
                                            "theta": {"field": val_axis, "type": "quantitative"},
                                            "color": {"field": cate_axis, "type": "nominal"}
                                        }
                                    },use_container_width=True)

                    st.divider()
                    
                st.button(f"Delete {vis_name}", key="delete_vis_"+str(i), on_click=delete_task, args = ['visualization', i])
                

            if i != len(pipeline_obj["visualization"]) - 1:
                st.divider()

        if len(pipeline_obj["visualization"]) == 0:
            st.write("No visualization found.")
            

        def add_visualization():
            task_name = "untitled"+str(len(current_pipeline_obj["visualization"])+1)
            current_pipeline_obj["visualization"].append({
                "name": task_name,
                "type": "Bar Chart",
                "input": "",
            })

        st.divider()
        st.button('Add Visualization', on_click=add_visualization, use_container_width=True)

    with code_view:
        pipeline_json = json.dumps(current_pipeline_obj, indent=4)
        code = pipeline_json
        st.code(code, language='json')



with preview_view:
    
    if "preview" in current_pipeline_obj:
        st.button("Rebuild Preview", on_click=build_pipeline_preview)
        preview_obj = current_pipeline_obj["preview"]
        if 'visualization' in preview_obj:
            vis_count = 1
            for task in preview_obj["visualization"]:                
                st.subheader(task)
                chart_settings = preview_obj["visualization"][task]
                chart_data = pd.DataFrame(preview_obj["serving"][chart_settings["input"]])
                ui_utils.show_chart(chart_settings, chart_data)
                
                vis_count += 1
                st.divider()
        if 'serving' in preview_obj:
            for task in preview_obj["serving"]:
                st.subheader(task)
                st.dataframe(pd.DataFrame(preview_obj["serving"][task]))
                st.divider()
        if 'standard' in preview_obj:
            for task in preview_obj["standard"]:
                st.subheader(task)
                st.dataframe(pd.DataFrame(preview_obj["standard"][task]))
                st.divider()
        if 'staging' in preview_obj:
            for task in preview_obj["staging"]:
                st.subheader(task)
                st.dataframe(pd.DataFrame(preview_obj["staging"][task]))
                st.divider()

        st.expander("JSON View")
        st.code(json.dumps(preview_obj, indent=4), language='json')

    else:
        st.button("Build Preview", on_click=build_pipeline_preview)






# st.sidebar.header(current_pipeline_obj["name"])
# st.sidebar.subheader("Staging Zone")
# for i in range(len(current_pipeline_obj["staging"])):
#     stg_name = current_pipeline_obj["staging"][i]["name"]
#     st.sidebar.markdown(f"[{stg_name}](#{stg_name})")
#     # st.sidebar.button(current_pipeline_obj["staging"][i]["name"], on_click=scroll_to_task, args = [current_pipeline_obj["staging"][i]["name"]])
# st.sidebar.subheader("Standardization Zone")
# for i in range(len(current_pipeline_obj["standard"])):
#     std_name = current_pipeline_obj["standard"][i]["name"]
#     st.sidebar.markdown(f"[{std_name}](#{std_name})")
#     # st.sidebar.button(current_pipeline_obj["standard"][i]["name"], on_click=scroll_to_task, args = [current_pipeline_obj["standard"][i]["name"]])
# st.sidebar.subheader("Serving Zone")
# for i in range(len(current_pipeline_obj["serving"])):
#     srv_name = current_pipeline_obj["serving"][i]["name"]
#     st.sidebar.markdown(f"[{srv_name}](#{srv_name})")
#     # st.sidebar.button(current_pipeline_obj["serving"][i]["name"], on_click=scroll_to_task, args = [current_pipeline_obj["serving"][i]["name"]])
# st.sidebar.subheader("Visualization")
# for i in range(len(current_pipeline_obj["visualization"])):
#     vis_name = current_pipeline_obj["visualization"][i]["name"]
#     st.sidebar.markdown(f"[{vis_name}](#{vis_name})")
#     # st.sidebar.button(current_pipeline_obj["visualization"][i]["name"], on_click=scroll_to_task, args = [current_pipeline_obj["visualization"][i]["name"]])