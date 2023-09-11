import streamlit as st
import time
import numpy as np
import glob
import os
import json
import pandas as pd
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.switch_page_button import switch_page
if "working_folder" not in st.session_state:
    switch_page("Home")
    
st.set_page_config(page_title="Workspace")

st.markdown("# Workspace")


working_folder = st.session_state["working_folder"]

# get all json files in working folder
json_files = glob.glob(os.path.join(working_folder, "*.json"))

def switch_pipeline(pipeline_obj):
    st.session_state["current_pipeline_obj"] = pipeline_obj
    switch_page("Editor")

for json_file in json_files:
    with open(json_file, "r") as f:
        with st.container():
            pipeline_obj = json.load(f)
            filename = os.path.basename(json_file)
            pipeline_id = pipeline_obj["id"]
            st.write(f"Project ID: {pipeline_id}")
            pipeline_name = pipeline_obj["name"]
            st.write(f"Project Name: {pipeline_name}")
            pipeline_description = pipeline_obj["description"]
            st.write(f"Project Description")
            st.markdown(pipeline_description)
            clicked = st.button("Edit", use_container_width=True, key=f"edit_{pipeline_id}")
            if clicked:
                st.session_state["current_pipeline_obj"] = pipeline_obj
                switch_page("Editor")
            st.divider()

