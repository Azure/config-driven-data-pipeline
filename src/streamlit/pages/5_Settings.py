import streamlit as st
import time
import numpy as np
import os
from streamlit_extras.colored_header import colored_header
st.set_page_config(page_title="Settings")

colored_header(
    label="Settings",
    description=f"Settings for CDDP",
    color_name="violet-70",
)

# get user home directory
usr_home = os.path.expanduser("~")
default_working_folder = st.session_state["working_folder"]
st.text_input("Working Folder", value=default_working_folder, key="working_folder", disabled=True)

st.subheader("Gallery Settings")
st.text_input("Gallery Account ID", value="", key="account_id")
st.text_input("Gallery SAS Token", value="", key="gallery_sas_token")

st.subheader("Azure OpenAI Settings")
st.text_input("Azure OpenAI Key", value="", key="azure_openai_key")
st.text_input("Azure OpenAI Deployment ID", value="", key="azure_openai_deployment_id") 

st.subheader("Azure Databricks Settings")
st.text_input("Azure Databricks Host", value="", key="azure_databricks_host")
st.text_input("Azure Databricks Token", value="", key="azure_databricks_token")



