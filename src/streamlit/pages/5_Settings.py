import streamlit as st
import time
import numpy as np
import os
import json
from streamlit_extras.colored_header import colored_header
from streamlit_extras.switch_page_button import switch_page
if "working_folder" not in st.session_state:
    switch_page("Home")

st.set_page_config(page_title="Settings")

colored_header(
    label="Settings",
    description=f"Settings for Config-Driven Data Pipeline",
    color_name="violet-70",
)
settings_path = os.path.join(st.session_state["working_folder"], ".settings.json")

def save_settings():
    with open(settings_path, "w") as f:
        settings_obj = {
            "account_id": st.session_state["account_id"],
            "gallery_token": st.session_state["gallery_token"],
            "azure_openai_key": st.session_state["azure_openai_key"],
            "azure_openai_deployment_id": st.session_state["azure_openai_deployment_id"],
            "azure_databricks_host": st.session_state["azure_databricks_host"],
            "azure_databricks_token": st.session_state["azure_databricks_token"],
        }
        f.write(json.dumps(settings_obj, indent=4))
        st.write(f"Settings saved in {settings_path}.")

st.button("Save Settings", key="save_settings", on_click=save_settings)

def remove_settings():
    if os.path.exists(settings_path):
        os.remove(settings_path)
        st.write(f"Settings removed from {settings_path}.")
    else:
        st.write(f"Settings not found in {settings_path}.")

st.button("Remove Settings", key="remove_settings", on_click=remove_settings)

def load_settings():

    if os.path.exists(settings_path):
        with open(settings_path, "r") as f:
            settings_obj = json.load(f)
            if "account_id" not in st.session_state:
                st.session_state["account_id"] = settings_obj["account_id"]
            if "gallery_token" not in st.session_state:
                st.session_state["gallery_token"] = settings_obj["gallery_token"]
            if "azure_openai_key" not in st.session_state:
                st.session_state["azure_openai_key"] = settings_obj["azure_openai_key"]
            if "azure_openai_deployment_id" not in st.session_state:
                st.session_state["azure_openai_deployment_id"] = settings_obj["azure_openai_deployment_id"]
            if "azure_databricks_host" not in st.session_state:
                st.session_state["azure_databricks_host"] = settings_obj["azure_databricks_host"]
            if "azure_databricks_token" not in st.session_state:
                st.session_state["azure_databricks_token"] = settings_obj["azure_databricks_token"]

    st.session_state["settings_initial_load"] = True
    st.write("Settings loaded.")


load_settings()


# st.write(st.session_state)

# get user home directory
usr_home = os.path.expanduser("~")
default_working_folder = st.session_state["working_folder"]
st.text_input("Working Folder", value=default_working_folder, disabled=True)

st.subheader("Gallery Settings")
st.text_input("Gallery Account ID", key="account_id")
st.text_input("Gallery Token", key="gallery_token")

st.subheader("Azure OpenAI Settings")
st.text_input("Azure OpenAI Key", key="azure_openai_key")
st.text_input("Azure OpenAI Deployment ID", key="azure_openai_deployment_id") 

st.subheader("Azure Databricks Settings")
st.text_input("Azure Databricks Host", key="azure_databricks_host")
st.text_input("Azure Databricks Token", key="azure_databricks_token")




