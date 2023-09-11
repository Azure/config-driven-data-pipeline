import cddp
import json
import streamlit as st


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


def update_sql(key, std_name):
    current_generated_std_sqls = st.session_state['current_generated_std_sqls']
    current_generated_std_sqls[std_name] = st.session_state[key]


def add_to_staging_zone(stg_name, stg_desc):
    pipeline_obj = st.session_state['current_pipeline_obj']
    current_generated_sample_data = st.session_state['current_generated_sample_data']
    sample_data = current_generated_sample_data.get(stg_name, None)

    if st.session_state[stg_name]:  # Add to staging zone if checkbox is checked
        if sample_data:
            spark = st.session_state["spark"]
            json_str, schema = cddp.load_sample_data(spark, json.dumps(sample_data), format="json")
            json_sample_data = json.loads(json_str)
            json_schema = json.loads(schema)
        else:
            json_sample_data = []
            json_schema = {}

        pipeline_obj["staging"].append({
            "name": stg_name,
            "description": stg_desc,
            "input": {
                "type": "filestore",
                "format": "json",
                "path": "",
                "read-type": "batch"
            },
            "output": {
                "target": stg_name,
                "type": ["file", "view"]
            },
            "schema": json_schema,
            "sampleData": json_sample_data
        })
    else:   # Remove staging task from staging zone if it's unchecked
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


def get_standardized_tables():
    pipeline_obj = st.session_state['current_pipeline_obj']
    standard = pipeline_obj.get("standard", None)

    if "current_std_srv_tables_schema" not in st.session_state:
        st.session_state['current_std_srv_tables_schema'] = {}
    current_std_srv_tables_schema = st.session_state['current_std_srv_tables_schema']

    if "standard" not in current_std_srv_tables_schema:
        current_std_srv_tables_schema["standard"] = {}

    standardized_table_names = []
    standardized_table_details = []
    if standard:
        for standardized_table in standard:
            std_name = standardized_table["output"]["target"]
            standardized_table_names.append(std_name)
            standardized_table_details.append({
                "table_name": std_name,
                "schema": current_std_srv_tables_schema["standard"].get(std_name, "")
            })

    return standardized_table_names, standardized_table_names
