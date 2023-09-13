import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
import cddp
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
from streamlit_extras.switch_page_button import switch_page

if "working_folder" not in st.session_state:
    switch_page("Home")

st.set_page_config(page_title="CDDP - Pipeline Gallery")


colored_header(
    label="Pipeline Gallery",
    description=f"Pipeline Gallery",
    color_name="violet-70",
)

settings_path = os.path.join(st.session_state["working_folder"], ".settings.json")
account_id = None
gallery_token = None
if os.path.exists(settings_path):
    with open(settings_path, "r") as f:
        settings_obj = json.load(f)
        gallery_token = settings_obj["gallery_token"]
        account_id = settings_obj["account_id"]

if gallery_token is None:
    st.error("Please set gallery token in settings.")
elif account_id is None:
    st.error("Please set account id in settings.")
else:
    all_pipelines = gallery_storage.load_all_pipelines(gallery_token)

    industry_list = ["Other", "Airlines", "Agriculture", "Automotive", "Banking", "Chemical", "Construction", "Education", "Energy", "Entertainment", "Food", "Government", "Healthcare", "Hospitality", "Insurance", "Machinery", "Manufacturing", "Media", "Mining", "Pharmaceutical", "Real Estate", "Retail", "Telecommunications", "Transportation", "Utilities", "Wholesale"]
    industry_tabs = st.tabs(industry_list)

    for i in range(len(industry_list)):
        industry = industry_list[i]
        with industry_tabs[i]:
            st.header(industry)
            pipelines = [pipeline for pipeline in all_pipelines if pipeline["industry"].lower() == industry.lower()]

            if len(pipelines) > 0:
                for pipeline in pipelines:
                    pipeline_id = pipeline["PartitionKey"]
                    pipeline_name = pipeline["name"]
                    pipeline_description = pipeline["description"]
                    st.write(f"Pipeline ID: {pipeline_id}")
                    st.write(f"Pipeline Name: {pipeline_name}")
                    st.write(f"Pipeline Description")
                    st.markdown(pipeline_description)
                    clicked = st.button("Fork", use_container_width=True, key=f"load_from_gallery_{pipeline_id}")
                    if clicked:
                        pipeline_obj = gallery_storage.load_pipeline_by_id(pipeline_id, account_id, gallery_token)
                        pipeline_obj['id'] = str(uuid.uuid4())
                        st.session_state["current_pipeline_obj"] = pipeline_obj
                        switch_page("Editor")
                    st.divider()


    # for pipeline in pipelines:
    #     with st.container():
    #         print(pipeline)
    #         pipeline_id = pipeline["PartitionKey"]
    #         pipeline_name = pipeline["name"]
    #         pipeline_description = pipeline["description"]
    #         st.write(f"Pipeline ID: {pipeline_id}")
    #         st.write(f"Pipeline Name: {pipeline_name}")
    #         st.write(f"Pipeline Description")
    #         st.markdown(pipeline_description)
    #         clicked = st.button("Fork", use_container_width=True, key=f"load_from_gallery_{pipeline_id}")
    #         if clicked:
    #             pipeline_obj = gallery_storage.load_pipeline_by_id(pipeline_id, account_id, gallery_token)
    #             pipeline_obj['id'] = str(uuid.uuid4())
    #             st.session_state["current_pipeline_obj"] = pipeline_obj
    #             switch_page("Editor")
    #         st.divider()

