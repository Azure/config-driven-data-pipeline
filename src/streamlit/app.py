import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import cddp
from cddp import openai_api
import streamlit as st
import streamlit_utils
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


def run_task(task_name, st_column, stage="standard",):
    dataframe = None
    with st_column:
        st.spinner("Executing...")
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
        if index < len(st.session_state["staged_tables"]):
            del st.session_state["staged_tables"][index]
    elif type == "standard":
        del current_pipeline_obj['standard'][index]
        if index < len(st.session_state["standardized_tables"]):
            del st.session_state["standardized_tables"][index]
    elif type == "serving":
        del current_pipeline_obj['serving'][index]
    elif type == "visualization":
        del current_pipeline_obj['visualization'][index]

    st.session_state['current_pipeline_obj'] = current_pipeline_obj 

def show_vis(vis_name, chart_type, serving_dataset_name, st_column):
    serving_dataset = run_task(serving_dataset_name, st_column, "serving")
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

if "current_generated_tables" not in st.session_state:
    st.session_state['current_generated_tables'] = {}

if "current_generated_sample_data" not in st.session_state:
    st.session_state['current_generated_sample_data'] = {}

if "current_generated_std_sqls" not in st.session_state:
    st.session_state['current_generated_std_sqls'] = {}

if "current_generated_srv_sqls" not in st.session_state:
    st.session_state['current_generated_srv_sqls'] = {}


current_pipeline_obj = st.session_state['current_pipeline_obj']
current_generated_tables = st.session_state['current_generated_tables']
current_generated_sample_data = st.session_state['current_generated_sample_data']
current_generated_std_sqls = st.session_state['current_generated_std_sqls']
current_generated_srv_sqls = st.session_state['current_generated_srv_sqls']

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
industry_list = ["Airlines", "Agriculture", "Automotive", "Banking", "Chemical", "Construction", "Education", "Energy", "Entertainment", "Food", "Government", "Healthcare", "Hospitality", "Insurance", "Machinery", "Manufacturing", "Media", "Mining", "Pharmaceutical", "Real Estate", "Retail", "Telecommunications", "Transportation", "Utilities", "Wholesale", "Other"]
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

ai_assistant_flag = st.toggle("AI Assistant")

# AI Assistnt for tables generation
tables = []
if ai_assistant_flag:
    with st.sidebar:
        st.write("Generate tables by AI assistant")
        generate_tables_col1, generate_tables_col2 = st.columns(2)
        with generate_tables_col1:
            st.button("Generate", on_click=streamlit_utils.click_button, kwargs={"button_name": "generate_tables"})
        
        if "generate_tables" not in st.session_state:
            st.session_state["generate_tables"] = False
        elif st.session_state["generate_tables"]:
            st.session_state["generate_tables"] = False     # Reset button clicked status
            with generate_tables_col2:
                with st.spinner('Generating...'):
                    tables = openai_api.recommend_tables_for_industry(pipeline_industry, pipeline_desc)
                    current_generated_tables["generated_tables"] = tables

        try:
            if "generated_tables" in current_generated_tables:
                tables = json.loads(current_generated_tables["generated_tables"])
            else:
                tables = []
            with st.sidebar:
                for table in tables:
                    columns = table["columns"]
                    columns_df = pd.DataFrame.from_dict(columns, orient='columns')

                    with st.expander(table["table_name"]):
                        st.checkbox("Add to staging zone", key=table["table_name"])
                        st.write(table["table_description"])
                        st.write(columns_df)
        except ValueError as e:
            st.write(tables)


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

selected_tables = streamlit_utils.get_selected_tables(tables)

if "staged_tables" not in st.session_state:
    st.session_state["staged_tables"] = []
staged_tables = st.session_state["staged_tables"]
with wizard_view:
    st.subheader('Staging Zone')



    pipeline_obj = st.session_state['current_pipeline_obj']

    
    for i in range(len(pipeline_obj["staging"]) ):
        
        target_name = pipeline_obj['staging'][i]['output']['target']
        stg_name = st.text_input(f'Dataset Name', key=f"stg_{i}_name", value=target_name)
        if stg_name:
            with st.expander(stg_name+" Settings"):
                selected_table = st.selectbox(
                    'Choose a dataset to add to staging zone',
                    selected_tables,
                    key=f'stg_{i}_ai_dataset')
                
                pipeline_obj['staging'][i]['output']['target'] = stg_name
                pipeline_obj['staging'][i]['name'] = stg_name

                if 'description' not in pipeline_obj['staging'][i]:
                    pipeline_obj['staging'][i]['description'] = ""
                stg_desc = st.text_area(f'Dataset Description', key=f"stg_{i}desc", value=pipeline_obj['staging'][i]['description'])
                if stg_desc:
                    pipeline_obj['staging'][i]['description'] = stg_desc

                upload_data_tab, generate_sample_data_tab = st.tabs(["Upload data", "Generate sample data"])
                spark = st.session_state["spark"]

                # Upload data files
                with upload_data_tab:
                    uploaded_file = st.file_uploader(f'Choose sample csv file', key=f'stg_{i}_file')
                    if uploaded_file is not None:

                        
                        dataframe = pd.read_csv(uploaded_file)
                        stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
                        data_str = stringio.read()
                        json_str, schema = cddp.load_sample_data(spark, data_str, format="csv")
                        pipeline_obj['staging'][i]['sampleData'] = json.loads(json_str)
                        pipeline_obj['staging'][i]['schema'] = json.loads(schema)


                    if 'sampleData' in pipeline_obj['staging'][i] and len(pipeline_obj['staging'][i]['sampleData']) > 0:
                        sampleData = pipeline_obj['staging'][i]['sampleData']
                        dataframe = pd.DataFrame(sampleData)
                        st.dataframe(dataframe)

                # Generate sample data by OpenAI
                with generate_sample_data_tab:
                    rows_count = st.slider("Number of rows", min_value=5, max_value=50, key=f'stg_{i}_rows_count_slider')
                    enable_data_requirements = st.toggle("With extra sample data requirements", key=f'stg_{i}_data_requirements_toggle')
                    data_requirements = ""
                    if enable_data_requirements:
                        data_requirements = st.text_area("Extra requirements for sample data",
                                                        key=f'stg_{i}_data_requirements_text_area',
                                                        placeholder="Exp: value of column X should follow patterns xxx-xxxx, while x could be A-Z or 0-9")

                    generate_sample_data_col1, generate_sample_data_col2 = st.columns(2)
                    with generate_sample_data_col1:
                        st.button("Generate",
                                key=f"generate_data_{i}",
                                on_click=streamlit_utils.click_button,
                                kwargs={"button_name": f"generate_sample_data_{selected_table}"})

                    sample_data = []
                    if f"generate_sample_data_{selected_table}" not in st.session_state:
                        st.session_state[f"generate_sample_data_{selected_table}"] = False
                    if f"{selected_table}_smaple_data_generated" not in st.session_state:
                        st.session_state[f"{selected_table}_smaple_data_generated"] = False
                    elif st.session_state[f"generate_sample_data_{selected_table}"]:
                        st.session_state[f"generate_sample_data_{selected_table}"] = False      # Reset clicked status
                        if not st.session_state[f"{selected_table}_smaple_data_generated"]:
                            with generate_sample_data_col2:
                                with st.spinner('Generating...'):
                                    table_details = streamlit_utils.get_selected_table_details(tables, selected_table)
                                    sample_data = openai_api.generate_sample_data(pipeline_industry, 
                                                                                  rows_count,
                                                                                  table_details,
                                                                                  data_requirements)
                                    st.session_state[f"{selected_table}_smaple_data_generated"] = True

                                    # Update CDDP json configs
                                    json_str, schema = cddp.load_sample_data(spark, sample_data)
                                    pipeline_obj['staging'][i]['sampleData'] = json.loads(json_str)
                                    pipeline_obj['staging'][i]['schema'] = json.loads(schema)

                                    # Update staged tables
                                    table_details["table_name"] = stg_name  # Update staged table name with given input value
                                    if table_details not in staged_tables:
                                        staged_tables.append(table_details)

                        if st.session_state[f"{selected_table}_smaple_data_generated"]:
                            st.session_state[f"{selected_table}_smaple_data_generated"] = False     # Reset data generated flag
                            json_sample_data = json.loads(sample_data)
                            current_generated_sample_data[stg_name] = json_sample_data      # Save generated data to session_state
                            st.session_state[f'stg_{i}_data'] = sample_data

                if stg_name in current_generated_sample_data:
                    sample_data_df = pd.DataFrame.from_dict(current_generated_sample_data[stg_name], orient='columns')
                    st.write(sample_data_df)

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




    staged_table_names = [table["table_name"] for table in staged_tables]
    st.subheader('Standardization Zone')

    if "standardized_tables" not in st.session_state:
        st.session_state["standardized_tables"] = []
    standardized_tables = st.session_state["standardized_tables"]

    for i in range(len(pipeline_obj["standard"])):
        standardized_table = {}
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

                selected_staged_tables = st.multiselect(
                    'Choose datasets to add to transformation',
                    staged_table_names,
                    key=f'std_{i}_ai_dataset')
                
                # Get selected staged table details
                selected_staged_table_details = streamlit_utils.get_selected_tables_details(staged_tables, selected_staged_tables)

                std_sql_val = ""
                transformation_logic_by_ai = st.toggle("AI Assistant", key=f"transformation_logic_{i}__by_ai")
                if transformation_logic_by_ai:
                    process_requirements = st.text_area("Transformation requirements", key=f"std_{i}_sql_requirements")
                    generate_sql_col1, generate_sql_col2 = st.columns(2)
                    with generate_sql_col1:
                        st.button(f'Generate SQL',
                                key=f'std_{i}_gen',
                                on_click=streamlit_utils.click_button,
                                kwargs={"button_name": f"std_{i}_gen_sql"})

                    if f'std_{i}_gen_sql' not in st.session_state:
                        st.session_state[f'std_{i}_gen_sql'] = False
                    if st.session_state[f'std_{i}_gen_sql']:
                        st.session_state[f'std_{i}_gen_sql'] = False    # Reset clicked status
                        with generate_sql_col2:
                            with st.spinner('Generating...'):
                                process_logic = openai_api.generate_custom_data_processing_logics(industry_name=pipeline_industry,
                                                                                                industry_contexts=pipeline_desc,
                                                                                                involved_tables=selected_staged_table_details,
                                                                                                custom_data_processing_logic=process_requirements,
                                                                                                output_table_name=std_name)
                                try:
                                    process_logic_json = json.loads(process_logic)
                                    std_sql_val = process_logic_json["sql"]
                                    current_generated_std_sqls[std_name] = std_sql_val

                                    # Update standardized_tables
                                    standardized_table = process_logic_json["schema"]
                                    if standardized_table["table_name"] not in [table["table_name"] for table in standardized_tables]:
                                        standardized_tables.append(standardized_table)
                                except ValueError as e:
                                    st.write(process_logic)
                else:
                    if len(current_pipeline_obj['standard'][i]['code']['sql']) == 0:
                        current_pipeline_obj['standard'][i]['code']['sql'].append("")
                    std_sql_val = current_pipeline_obj['standard'][i]['code']['sql'][0]
                    current_generated_std_sqls[std_name] = std_sql_val
                
                std_sql = st.text_area(f'SQL',
                                       key=f'std_{i}_sql',
                                       value=current_generated_std_sqls[std_name],
                                       on_change=streamlit_utils.update_sql,
                                       args=[f'std_{i}_sql', current_pipeline_obj, "standard", i])
                if std_sql:
                    pipeline_obj['standard'][i]['code']['sql'][0] = std_sql

                run_sql_col1, run_sql_col2 = st.columns(2)
                with run_sql_col1:
                    st.button('Run SQL', key=f'std_{i}_run_sql', on_click=run_task, args = [std_name, run_sql_col2, "standard"])

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



    standardized_table_names = [table["table_name"] for table in standardized_tables]
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

                selected_stg_std_tables = st.multiselect(
                    'Choose datasets to add to aggregation',
                    standardized_table_names + staged_table_names,
                    key=f'srv_{i}_ai_dataset')
                
                # Get selected staged or standardized table details
                selected_stg_std_table_details = streamlit_utils.get_selected_tables_details(staged_tables + standardized_tables,
                                                                                             selected_stg_std_tables)
                srv_sql_val = ""
                aggregation_logic_by_ai = st.toggle("AI Assistant", key=f"aggregation_logic_{i}_by_ai")
                if aggregation_logic_by_ai:
                    agg_process_requirements = st.text_area("Aggregation requirements", key=f"srv_{i}_sql_requirements")
                    generate__agg_sql_col1, generate_agg_sql_col2 = st.columns(2)
                    with generate__agg_sql_col1:
                        st.button(f'Generate SQL',
                                  key=f'srv_{i}_gen',
                                  on_click=streamlit_utils.click_button,
                                  kwargs={"button_name": f"srv_{i}_gen_sql"})
                        
                    if f'srv_{i}_gen_sql' not in st.session_state:
                        st.session_state[f'srv_{i}_gen_sql'] = False
                    if st.session_state[f'srv_{i}_gen_sql']:
                        st.session_state[f'srv_{i}_gen_sql'] = False    # Reset clicked status
                        with generate_agg_sql_col2:
                            with st.spinner('Generating...'):
                                agg_process_logic = openai_api.generate_custom_data_processing_logics(industry_name=pipeline_industry,
                                                                                                      industry_contexts=pipeline_desc,
                                                                                                      involved_tables=selected_stg_std_table_details,
                                                                                                      custom_data_processing_logic=agg_process_requirements,
                                                                                                      output_table_name=srv_name)
                                try:
                                    agg_process_logic_json = json.loads(agg_process_logic)
                                    srv_sql_val = agg_process_logic_json["sql"]
                                    current_generated_srv_sqls[srv_name] = srv_sql_val
                                except ValueError as e:
                                    st.write(agg_process_logic)
                else:
                    if len(current_pipeline_obj['serving'][i]['code']['sql']) == 0:
                        current_pipeline_obj['serving'][i]['code']['sql'].append("")
                    srv_sql_val = current_pipeline_obj['serving'][i]['code']['sql'][0]
                    current_generated_srv_sqls[srv_name] = srv_sql_val
                
                srv_sql = st.text_area(f'SQL',
                                       key=f'srv_{i}_sql',
                                       value=current_generated_srv_sqls[srv_name],
                                       on_change=streamlit_utils.update_sql,
                                       args=[f'srv_{i}_sql', current_pipeline_obj, "serving", i])
                if srv_sql:
                    pipeline_obj['serving'][i]['code']['sql'][0] = srv_sql

                run_agg_sql_col1, run_agg_sql_col2 = st.columns(2)
                with run_agg_sql_col1:
                    st.button(f'Run SQL', key=f'run_srv_{i}_sql', on_click=run_task, args = [srv_name, run_agg_sql_col2, "serving"])
                
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
                    
                st.button(f'Run Visualization',
                          key=f'run_vis_{i}',
                          on_click=show_vis,
                          args = [vis_name, chart_type, pipeline_obj['visualization'][i]['input'], run_agg_sql_col2])
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
