import json
import streamlit as st


def get_selected_tables(tables):
    selected_tables = []
    if len(tables) > 0:
        for table in tables:
            table_name = table["table_name"]
            if st.session_state[table["table_name"]]:
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
