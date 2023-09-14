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
import utils.ui_utils as ui_utils
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

industry_pipelines = {}
industry_list = ["Airlines", "Agriculture", "Automotive", "Banking", "Chemical", "Construction", "Education", "Energy", "Entertainment", "Food", "Government", "Healthcare", "Hospitality", "Insurance", "Machinery", "Manufacturing", "Media", "Mining", "Pharmaceutical", "Real Estate", "Retail", "Telecommunications", "Transportation", "Utilities", "Wholesale", "Other"]
for i in range(len(industry_list)):
    industry = industry_list[i]
    pipelines = [pipeline for pipeline in all_pipelines if pipeline["industry"].lower() == industry.lower()]
    industry_pipelines[industry] = pipelines

selected_industry_list  = [industry for industry in industry_list if len(industry_pipelines[industry]) > 0]

industry_tabs = st.tabs(selected_industry_list)

for i in range(len(selected_industry_list)):
    industry = selected_industry_list[i]
    with industry_tabs[i]:
        st.header(industry)
        pipelines = industry_pipelines[industry]

        if len(pipelines) > 0:
            # sort pipelines by date
            pipelines = sorted(pipelines, key=lambda k: k['publish_date'], reverse=True)

            for pipeline in pipelines:
                pipeline_id = pipeline["PartitionKey"]
                pipeline_name = pipeline["name"]
                pipeline_publish_date = pipeline["publish_date"]
                pipeline_account_id = pipeline["account_id"]
                pipeline_description = pipeline["description"][:200] + "..."

                st.header(pipeline_name)

                col1, col2 = st.columns(2)
                with col1:
                    st.caption(f"Pipeline ID: {pipeline_id}")
                    st.caption(f"*Published at {pipeline_publish_date}*")
                    if pipeline_account_id.find("@") > 0:
                        st.markdown(f"**Author**: *{pipeline_account_id.split('@')[0]}*")
                    else:
                        st.markdown(f"**Author**: *{pipeline_account_id}*")
                    
                    st.markdown("**Pipeline Description**")
                    st.markdown(pipeline_description)
                
                with col2:
                    try:
                        pipeline_body = gallery_storage.load_pipeline_by_id(pipeline_id, pipeline_account_id, gallery_token)
                        preview_obj = pipeline_body["preview"]
                        if "visualization" in preview_obj:
                            for task in preview_obj["visualization"]:                
                                st.caption(task)
                                chart_settings = preview_obj["visualization"][task]
                                chart_data = pd.DataFrame(preview_obj["serving"][chart_settings["input"]])
                                ui_utils.show_chart(chart_settings, chart_data)
                                break

                    except Exception as ex:
                        print(f"can not show diagram: {ex}")
                        st.error("Can not show diagram correctly.")

                btn_text = "Fork"
                if pipeline_account_id == account_id:
                    btn_text = "Edit"
                clicked = st.button(btn_text, use_container_width=True, key=f"load_from_gallery_{pipeline_id}")
                if clicked:
                    pipeline_obj = gallery_storage.load_pipeline_by_id(pipeline_id, pipeline_account_id, gallery_token)
                    if pipeline_account_id != account_id:
                        pipeline_obj['id'] = str(uuid.uuid4())
                    st.session_state["current_pipeline_obj"] = pipeline_obj
                    switch_page("Editor")
                st.divider()
