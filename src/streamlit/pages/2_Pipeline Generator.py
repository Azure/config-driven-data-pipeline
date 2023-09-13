import cddp
import json
import os
import pandas as pd
import streamlit as st
import streamlit_utils
import sys
import numpy as np
from cddp import openai_api


sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
import cddp.openai_api as openai_api
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.colored_header import colored_header
from streamlit_extras.chart_container import chart_container
if "current_pipeline_obj" not in st.session_state:
    switch_page("Home")

current_pipeline_obj = st.session_state["current_pipeline_obj"]

if "spark" not in st.session_state:
  spark = cddp.create_spark_session()
  st.session_state["spark"] = spark

st.set_page_config(page_title="AI Assiatant")

colored_header(
    label="Pipeline Generator",
    description=f"Leverage AI to generate a pipeline for you!",
    color_name="violet-70",
)

industry_list = ["Other", "Airlines", "Agriculture", "Automotive", "Banking", "Chemical", "Construction", "Education", "Energy", "Entertainment", "Food", "Government", "Healthcare", "Hospitality", "Insurance", "Machinery", "Manufacturing", "Media", "Mining", "Pharmaceutical", "Real Estate", "Retail", "Telecommunications", "Transportation", "Utilities", "Wholesale"]
pipeline_industry = st.selectbox('Industry', industry_list, key='gen_industry')
pipeline_desc = st.text_area('Pipeline description', key='gen_pipeline_description')

tab_usecase, tab_stg, tab_std, tab_srv, tab_vis = st.tabs(['Use Cases', 'Staging', 'Standardization', 'Serving', 'Visualization'])


if 'gen_pipeline_usecases' not in st.session_state:
    st.session_state['gen_pipeline_usecases'] = []

with tab_usecase:
  if st.button("Generate Usecases"):
    st.session_state['gen_pipeline_usecases'] = openai_api.recommend_data_processing_scenario_mock(pipeline_industry)

  if st.session_state['gen_pipeline_usecases']:
    try:
      usecases = json.loads(st.session_state['gen_pipeline_usecases'])
      for index, usecase in enumerate(usecases):
        st.checkbox(usecase['pipeline_name'], key=f"usecase_checkbox_{index}")
        st.write(usecase['description'])
    except ValueError as e:
      st.write(st.session_state['gen_pipeline_usecases'])


selected_usecases = []
if st.session_state['gen_pipeline_usecases']:
  selected_usecases = streamlit_utils.get_selected_usecases(json.loads(st.session_state['gen_pipeline_usecases']))

if 'gen_tables_details' not in st.session_state:
    st.session_state['gen_tables_details'] = []

with tab_stg: 
  for index, selected_usecase in enumerate(selected_usecases):
    st.write(f"Pipeline name: {selected_usecase['pipeline_name']}")
    st.write(f"Pipeline description: {selected_usecase['description']}")

    if st.button("Generate Sample Data", key=f"gen_table_and_data_{index}"):
      st.session_state['gen_tables_details'] = openai_api.recommend_tables_and_data_for_industry_mock(selected_usecase['pipeline_name'],
                                                                                                  selected_usecase['description'])
      # Update current_pipeline_obj
      current_pipeline_obj["name"] = selected_usecase["pipeline_name"].replace(" ", "_")
      current_pipeline_obj["description"] = selected_usecase["description"]
      current_pipeline_obj["industry"] = pipeline_industry

    if st.session_state['gen_tables_details']:
      try:
        gen_tables_details = json.loads(st.session_state['gen_tables_details'])
        for index, gen_table_details in enumerate(gen_tables_details):
          with st.expander(gen_table_details["table_name"]):
            st.write(gen_table_details["table_description"])
            schema_df = pd.DataFrame.from_dict(gen_table_details["schema"], orient='columns')
            st.write(schema_df)
            
            st.divider()
            st.write("Sample data")
            sample_data_df = pd.DataFrame.from_dict(gen_table_details["sample_data"], orient='columns')
            st.write(sample_data_df)

            st.checkbox('Add to staging zone',
                        key=f'gen_table_and_data_checkbox_{index}',
                        on_change=streamlit_utils.add_table_to_staging_zone,
                        args=[f"gen_table_and_data_checkbox_{index}", gen_table_details["table_name"], gen_table_details["sample_data"]])
      except ValueError as e:
        st.write(st.session_state['gen_tables_details'])

    st.divider()


if 'staged_tables' not in st.session_state:
    staged_table_names, staged_table_details = streamlit_utils.get_staged_tables()

if 'gen_std_details' not in st.session_state:
    st.session_state['gen_std_details'] = []

if "current_generated_std_srv_sqls" not in st.session_state:
    st.session_state['current_generated_std_srv_sqls'] = {}
current_generated_std_srv_sqls = st.session_state['current_generated_std_srv_sqls']

with tab_std:
  st.write(f"Selected tables in staging zone: {', '.join(staged_table_names)}")
  if st.button("Generate Transformations"):
      st.session_state['gen_std_details'] = openai_api.recommend_data_processing_logics(industry_name=pipeline_industry,
                                                                                        pipeline_description=current_pipeline_obj["description"],
                                                                                        involved_tables=staged_table_details,
                                                                                        processing_logic="cleansing or transformation")

  if st.session_state['gen_std_details']:
    try:
      gen_std_details = json.loads(st.session_state['gen_std_details'])

      for index, gen_std in enumerate(gen_std_details):
        output_table = gen_std['output_table']
        current_generated_std_srv_sqls[output_table] = gen_std["sql"]

        with st.expander(gen_std["description"]):
          involved_tables = gen_std["involved_tables"]
          st.write(f"Involved tables: {', '.join(involved_tables)}")
          st.write(f"Output table: {gen_std['output_table']}")  
          st.text_area("Generated SQL", gen_std["sql"])
          st.checkbox('Add to standardization zone',
                    key=f'add_to_std_checkbox_{index}',
                    on_change=streamlit_utils.add_to_std_srv_zone,
                    args=[f"add_to_std_checkbox_{index}", f"{output_table}", gen_std["description"], "standard"])
    except ValueError as e:
        st.write(st.session_state['gen_std_details'])

    st.write(current_pipeline_obj)


with tab_srv:
  st.button("Generate Serving")
      # for item in SQL:
      #   st.checkbox(item['Description'])
      #   st.code(item['Statement'])


with tab_vis:
  if st.button("Generate Visualization"):
      for i in range(10):
        chart_data = pd.DataFrame(
          np.random.randn(50, 10),
          columns=('col %d' % i for i in range(10)))
        st.checkbox(f'Chart {i}', key=f'gen_chart_{i}')
        rand = np.random.randint(0, 3)
        with chart_container(chart_data):
          if rand == 0:
            st.area_chart(chart_data)
          elif rand == 1:
             st.bar_chart(chart_data)
          elif rand == 2:
             st.line_chart(chart_data)
