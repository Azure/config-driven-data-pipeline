import streamlit as st
import os
import streamlit_utils

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)

st.write("# Welcome to CDDP!👋")

if "working_folder" not in st.session_state:
    # get user home directory
    usr_home = os.path.expanduser("~")
    default_working_folder = os.path.join(usr_home, "Documents", "CDDP")
    st.session_state["working_folder"] = default_working_folder
    if not os.path.exists(default_working_folder):
        os.makedirs(default_working_folder)



if "current_pipeline_obj" not in st.session_state:
    streamlit_utils.create_pipeline()


