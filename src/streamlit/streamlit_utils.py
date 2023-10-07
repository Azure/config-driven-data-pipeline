import cddp
import json
import random
import streamlit as st
import tempfile
import uuid
import pandas as pd
from time import sleep


def get_selected_tables(tables):
    selected_tables = []
    if len(tables) > 0:
        for table in tables:
            table_name = table["table_name"]
            if table_name in st.session_state and st.session_state[table_name]:
                selected_tables.append(table_name)

    return selected_tables


def click_button(button_name):
    st.session_state[button_name] = True


def get_selected_table_details(tables, table_name):
    target_table = {}
    for table in tables:
        if table["table_name"] == table_name:
            target_table = table
    
    return target_table


def get_selected_tables_details(tables, table_names):
    target_tables = []
    if table_names:
        for table_name in table_names:
            for table_details in tables:
                if table_details["table_name"] == table_name:
                    target_tables.append(table_details)
    
    return target_tables


def is_json_string(input: str):
    is_json = True
    try:
        json.loads(input)
    except ValueError as e:
        is_json = False

    return is_json


def update_sql(key, table_name):
    current_generated_std_srv_sqls = st.session_state['current_generated_std_srv_sqls']
    current_generated_std_srv_sqls[table_name] = st.session_state[key]


def add_to_staging_zone(gen_table_index, stg_name, stg_desc):
    pipeline_obj = st.session_state['current_pipeline_obj']
    current_generated_sample_data = st.session_state['current_generated_sample_data']
    sample_data = current_generated_sample_data.get(stg_name, None)
    generated_tables = st.session_state["current_generated_tables"]["generated_tables"]

    if st.session_state[f"add_to_staging_{gen_table_index}_checkbox"]:  # Add to staging zone if checkbox is checked
        generated_tables[gen_table_index]["staged_flag"] = True

        if sample_data:
            spark = st.session_state["spark"]
            json_str, schema = cddp.load_sample_data(spark, json.dumps(sample_data), format="json")
            json_sample_data = json.loads(json_str)
            json_schema = json.loads(schema)
        else:
            json_sample_data = []
            json_schema = {}

        add_stg_dataset(pipeline_obj, stg_name, json_schema, json_sample_data)
    else:   # Remove staging task from staging zone if it's unchecked
        generated_tables[gen_table_index]["staged_flag"] = False
        for index, obj in enumerate(pipeline_obj["staging"]):
            if obj["name"] == stg_name:
                del pipeline_obj['staging'][index]


def get_staged_tables():
    pipeline_obj = st.session_state['current_pipeline_obj']
    staging = pipeline_obj.get("staging", None)

    staged_table_names = []
    staged_table_details = []
    if staging:
        for staged_table in staging:
            staged_table_names.append(staged_table["output"]["target"])
            staged_table_details.append({
                "table_name": staged_table["output"]["target"],
                "schema": staged_table.get("schema", "")
            })
            
    return staged_table_names, staged_table_details


def add_to_std_srv_zone(button_key, std_srv_name, std_srv_desc, zone):
    pipeline_obj = st.session_state["current_pipeline_obj"]
    current_generated_std_srv_sqls = st.session_state["current_generated_std_srv_sqls"]

    if st.session_state[button_key]:  # Add to std or srv zone if click add-to-std/srv-zone button
        pipeline_obj[zone].append({
            "name": std_srv_name,
            "type": "batch",
            "description": std_srv_desc,
            "code": {
                "lang": "sql",
                "sql": [current_generated_std_srv_sqls[std_srv_name]]
            },
            "output": {
                "target": std_srv_name,
                "type": ["file", "view"]
            },
            "dependency": []
        })
    else:   # Remove staging task from staging zone if it's unchecked
        for index, obj in enumerate(pipeline_obj[zone]):
            if obj["name"] == std_srv_name:
                del pipeline_obj['standard'][index]


def add_std_srv_schema(zone, output_table_name, schema):
    if "current_std_srv_tables_schema" not in st.session_state:
        st.session_state['current_std_srv_tables_schema'] = {}
    current_std_srv_tables_schema = st.session_state['current_std_srv_tables_schema']
    
    current_std_srv_tables_schema[zone] = {}
    current_std_srv_tables_schema[zone][output_table_name] = schema


def get_std_srv_tables(task_type):
    pipeline_obj = st.session_state['current_pipeline_obj']
    tasks = pipeline_obj.get(task_type, None)
    current_editing_pipeline_tasks = st.session_state['current_editing_pipeline_tasks']

    std_srv_table_names = []
    std_srv_table_details = []
    if tasks:
        for std_srv_table in tasks:
            task_name = std_srv_table["output"]["target"]
            std_srv_table_names.append(task_name)
            for index, task in enumerate(current_editing_pipeline_tasks[task_type]):
                if task['target'] == task_name:
                    std_srv_table_details.append({
                        "table_name": task_name,
                        "schema": current_editing_pipeline_tasks[task_type][index].get('query_results_schema', '')
                    })

    return std_srv_table_names, std_srv_table_details


def create_pipeline():
    proj_id = str(uuid.uuid4())
    st.session_state['current_pipeline_obj'] = {
        "name": "Untitiled",
        "id": proj_id, 
        "description": "",
        "industry": "Other",
        "staging": [],
        "standard": [],
        "serving": [],
        "visualization": []
    }
    return st.session_state['current_pipeline_obj']


def add_stg_dataset(pipeline_obj, task_name, schema={}, sample_data=[]):
    pipeline_obj["staging"].append({
        "name": task_name,
        "description": "",
        "input": {
            "type": "filestore",
            "format": "csv",
            "path": f"/FileStore/cddp_apps/{pipeline_obj['id']}/landing/{task_name}",
            "read-type": "batch"
        },
        "output": {
            "target": task_name,
            "type": ["file", "view"]
        },
        "schema": schema,
        "sampleData": sample_data
    })


def run_task(task_name, stage="standard", index=None):
    dataframe = None
    current_editing_pipeline_tasks = st.session_state['current_editing_pipeline_tasks']
    try:
        spark = st.session_state["spark"]
        config = st.session_state["current_pipeline_obj"]
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
                    # print(dataframe)
                    current_editing_pipeline_tasks[stage][index]['sql_query_results'] = dataframe

                    res_schema = res_df.schema.json()
                    current_editing_pipeline_tasks[stage][index]['query_results_schema'] = json.loads(res_schema)

    except Exception as e:
        st.error(f"Cannot run task: {e}")

    return dataframe


def add_transformation():
    task_name = "untitled"+str(len(st.session_state["current_pipeline_obj"]["standard"])+1)
    st.session_state["current_pipeline_obj"]["standard"].append({
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


def delete_task(type, index):
    current_pipeline_obj = st.session_state["current_pipeline_obj"]

    if type == "staging":
        del current_pipeline_obj['staging'][index]
    elif type == "standard":
        del current_pipeline_obj['standard'][index]
        del st.session_state['current_editing_pipeline_tasks']['standard'][index]
    elif type == "serving":
        del current_pipeline_obj['serving'][index]
        del st.session_state['current_editing_pipeline_tasks']['serving'][index]
    elif type == "visualization":
        del current_pipeline_obj['visualization'][index]

    st.session_state['current_pipeline_obj'] = current_pipeline_obj 


def update_selected_tables(task_type, index, multiselect_key):
    st.session_state['current_editing_pipeline_tasks'][task_type][index]['involved_tables'] = st.session_state[multiselect_key]


def add_aggregation():
    task_name = "untitled"+str(len(st.session_state["current_pipeline_obj"]["serving"])+1)
    st.session_state["current_pipeline_obj"]["serving"].append({
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


def has_staged_table():
    has_staged_table = False
    if "generated_tables" in st.session_state["current_generated_tables"]:
        generated_tables = st.session_state["current_generated_tables"]["generated_tables"]
        for table in generated_tables:
            if "staged_flag" in table and table["staged_flag"]:
                has_staged_table = True
                break

    return has_staged_table


def check_tables_dependency(target_name):
    has_dependency = False
    current_editing_pipeline_tasks = st.session_state['current_editing_pipeline_tasks']
    current_used_std_tables = []
    current_used_srv_tables = []
    for task in current_editing_pipeline_tasks['standard']:
        current_used_std_tables += task.get('involved_tables', [])
    for task in current_editing_pipeline_tasks['serving']:
        current_used_srv_tables += task.get('involved_tables', [])

    if target_name in current_used_std_tables + current_used_srv_tables:
        has_dependency =  True

    return has_dependency


def widget_on_change(widget_key, index, session_state_key):
    current_generated_tables = st.session_state['current_generated_tables']['generated_tables']
    current_generated_tables[index][session_state_key] = st.session_state[widget_key]
    st.session_state["generate_tables"] = False


def render_table_expander(table,
                          current_generated_tables,
                          current_generated_sample_data,
                          current_pipeline_obj,
                          gen_table_index,
                          industry_name,
                          openai_api,
                          key_suffix="init"):
    sample_data = None
    added_to_stage = current_generated_tables["generated_tables"][gen_table_index].get("staged_flag", False)
    expander_label = table["table_name"]

    if added_to_stage:
        expander_label += "     :heavy_check_mark:"

    with st.expander(expander_label, expanded=added_to_stage):
        gen_table_name = table["table_name"]
        gen_table_desc = table["table_description"]
        stg_name_has_dependency = check_tables_dependency(gen_table_name)

        gen_table_sample_data_count = current_generated_tables["generated_tables"][gen_table_index].get("sample_data_count", 5)
        sample_data_requirements_flag = current_generated_tables["generated_tables"][gen_table_index].get("sample_data_requirements_flag", False)
        data_requirements = current_generated_tables["generated_tables"][gen_table_index].get("data_requirements", "")
        

        columns = table["columns"]
        columns_df = pd.DataFrame.from_dict(columns, orient='columns')

        st.write(gen_table_desc)
        st.write(columns_df)

        st.write(f"Generate sample data")
        rows_count = st.slider("Number of rows",
                               min_value=5,
                               max_value=50,
                               value=gen_table_sample_data_count,
                               key=f'gen_rows_count_slider_{gen_table_name}_{key_suffix}',
                               on_change=widget_on_change,
                               args=[f'gen_rows_count_slider_{gen_table_name}_{key_suffix}',
                                     gen_table_index,
                                     'sample_data_count'],
                               disabled=st.session_state["disable_generate_data_widget"])

        enable_data_requirements = st.toggle("With extra sample data requirements",
                                             value=sample_data_requirements_flag,
                                             key=f'data_requirements_toggle_{gen_table_name}_{key_suffix}',
                                             on_change=widget_on_change,
                                             args=[f'data_requirements_toggle_{gen_table_name}_{key_suffix}',
                                                   gen_table_index,
                                                   'sample_data_requirements_flag'],
                                             disabled=st.session_state["disable_generate_data_widget"])

        if enable_data_requirements:
            data_requirements = st.text_area("Extra requirements for sample data",
                                             value=data_requirements,
                                             key=f'data_requirements_text_area_{gen_table_name}_{key_suffix}',
                                             placeholder="Exp: value of column X should follow patterns xxx-xxxx, while x could be A-Z or 0-9",
                                             on_change=widget_on_change,
                                             args=[f'data_requirements_text_area_{gen_table_name}_{key_suffix}',
                                                   gen_table_index,
                                                   'data_requirements'],
                                             disabled=st.session_state["disable_generate_data_widget"])

        generate_sample_data_col1, generate_sample_data_col2 = st.columns(2)
        with generate_sample_data_col1:
            st.button("Generate Sample Data",
                      key=f"generate_data_button_{gen_table_name}_{key_suffix}",
                      on_click=click_button,
                      kwargs={"button_name": f"generate_sample_data_{gen_table_name}"},
                      disabled=st.session_state["disable_generate_data_widget"],
                      use_container_width=True)

        if f"generate_sample_data_{gen_table_name}" not in st.session_state:
            st.session_state[f"generate_sample_data_{gen_table_name}"] = False

        if f"{gen_table_name}_smaple_data_generated" not in st.session_state:
            st.session_state[f"{gen_table_name}_smaple_data_generated"] = False
        elif st.session_state[f"generate_sample_data_{gen_table_name}"]:
            st.session_state[f"generate_sample_data_{gen_table_name}"] = False      # Reset clicked status
            if not st.session_state[f"{gen_table_name}_smaple_data_generated"]:
                with generate_sample_data_col2:
                    with st.spinner('Generating...'):
                        sample_data = openai_api.generate_sample_data(industry_name, 
                                                                      rows_count,
                                                                      table,
                                                                      data_requirements)
                        st.session_state[f"{gen_table_name}_smaple_data_generated"] = True
                        # Store generated data to session_state
                        current_generated_sample_data[gen_table_name] = sample_data

                        # Also update current_pipeline_obj if checked check-box before generating sample data
                        # if sample_data and st.session_state[f"add_to_staging_{gen_table_index}_checkbox"]:
                        if sample_data and st.session_state.get(f"add_to_staging_{gen_table_index}_checkbox", False):
                            spark = st.session_state["spark"]
                            json_str, schema = cddp.load_sample_data(spark, sample_data, format="json")

                            for index, dataset in enumerate(current_pipeline_obj['staging']):
                                if dataset["name"] == gen_table_name:
                                    i = index

                            current_pipeline_obj['staging'][i]['sampleData'] = json.loads(json_str)
                            current_pipeline_obj['staging'][i]['schema'] = json.loads(schema)

            if st.session_state[f"{gen_table_name}_smaple_data_generated"]:
                st.session_state[f"{gen_table_name}_smaple_data_generated"] = False     # Reset data generated flag
                json_sample_data = json.loads(sample_data)
                current_generated_sample_data[gen_table_name] = json_sample_data      # Save generated data to session_state

        if gen_table_name in current_generated_sample_data:
            sample_data_df = pd.DataFrame.from_dict(current_generated_sample_data[gen_table_name], orient='columns')
            st.write(sample_data_df)

            if stg_name_has_dependency:
                st.info("""This table has been referenced by other tasks!
                        Please remove relevant dependency before trying to remove it from staging zone.""")
            
            # Show checkbox only after sample data has been generated      
            st.checkbox("Add to staging zone",
                        key=f"add_to_staging_{gen_table_index}_checkbox",
                        value=added_to_stage,
                        on_change=add_to_staging_zone,
                        args=[gen_table_index, gen_table_name, gen_table_desc],
                        disabled=stg_name_has_dependency)


def generate_tables(placeholder,
                    current_generated_tables,
                    current_pipeline_obj,
                    current_generated_sample_data,
                    industry_name,
                    industry_contexts,
                    openai_api):
    st.session_state["generate_tables"] = True
    gen_tables_count = st.session_state["generate_tables_count"] = random.randint(5, 8)
    if "disable_generate_data_widget" not in st.session_state or not st.session_state["disable_generate_data_widget"]:
        st.session_state["disable_generate_data_widget"] = True

    # Clean existing table expanders before render new generate ones
    placeholder.empty()
    sleep(0.01) # Workaround for elements empty/cleaning issue, https://github.com/streamlit/streamlit/issues/5044

    with placeholder.container():
        spinner_container = st.empty().container()
        if not has_staged_table():  # Generate new tables if there's no existing dependency/references found in std/srv tasks 
            # Clean current_generated_sample_data key in session state
            del st.session_state['current_generated_sample_data']
            current_generated_sample_data = {}

            gen_table_index = 0
            generated_tables = ""
            current_generated_tables["generated_tables"] = []

            while gen_table_index < gen_tables_count:
                with spinner_container:
                    with st.spinner(f'Generating {gen_table_index + 1} of {gen_tables_count} tables...'):
                        table = openai_api.recommend_tables_for_industry_one_at_a_time(industry_name, industry_contexts, generated_tables)

                current_generated_tables["generated_tables"].append(json.loads(table))
                generated_tables = json.dumps(current_generated_tables["generated_tables"], indent=2)

                render_table_expander(json.loads(table),
                                      current_generated_tables,
                                      current_generated_sample_data,
                                      current_pipeline_obj,
                                      gen_table_index,
                                      industry_name,
                                      openai_api)

                gen_table_index += 1

    # Clean the temporary generated tables and redraw them again in 2_AI_Assistant.py to enable all widgets inside table expanders.
    placeholder.empty()
    sleep(0.01)
