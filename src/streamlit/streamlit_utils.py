import cddp
import json
import streamlit as st
import tempfile
import uuid


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
