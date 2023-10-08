import cddp
import json
import os
import pandas as pd
import streamlit as st
import sys
import streamlit_utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from cddp.openai_api import OpenaiApi
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.colored_header import colored_header
from time import sleep

if "current_pipeline_obj" not in st.session_state:
    switch_page("Home")

current_pipeline_obj = st.session_state["current_pipeline_obj"]


st.set_page_config(page_title="AI Assistant")

colored_header(
    label="AI Assistant",
    description=f"Leverage AI to assist you in data pipeline development",
    color_name="violet-70",
)

openai_api = OpenaiApi()



# Display pipeline basic info fetching from Editor page if it's maintained there
# initialize current_pipeline_obj key in session state at Home.py
# current_pipeline_obj = {}
# if "current_pipeline_obj" in st.session_state:
#     current_pipeline_obj = st.session_state["current_pipeline_obj"]
# else:
#     st.session_state["current_pipeline_obj"] = {}

st.write(f'Pipeline Name: {current_pipeline_obj.get("name", "")}')
st.write(f'Industry: {current_pipeline_obj.get("industry", "")}')
st.write(f'Desciption: {current_pipeline_obj.get("description", "")}')


use_case_tab, tab_data, tab_std_sql, tab_srv_sql = st.tabs(["Use Case", "Sample Data", "Standardization SQL", "Aggregation SQL"])

with use_case_tab:
    if "current_generated_usecases" not in st.session_state:
        st.session_state['current_generated_usecases'] = {}

    generate_use_cases_col1, generate_use_cases_col2 = st.columns(2)
    with generate_use_cases_col1:
        st.button("Generate Use Case", on_click=streamlit_utils.click_button, kwargs={"button_name": "generated_usecases"}, use_container_width=True)

    if "generated_usecases" not in st.session_state:
        st.session_state["generated_usecases"] = False
    elif st.session_state["generated_usecases"]:
        st.session_state["generated_usecases"] = False     # Reset button clicked status
        with generate_use_cases_col2:
            with st.spinner('Generating...'):
                try:
                    usecases = openai_api.recommend_data_processing_scenario(current_pipeline_obj.get("industry", ""))
                    st.session_state['current_generated_usecases'] = json.loads(usecases)
                except ValueError as e:
                    st.error("Got invalid response from AI Assistant, please try again!")
                except Exception as e:
                    st.error("Got error while getting help from AI Assistant, please try again!")
    
    if "current_generated_usecases" in st.session_state:
        usecases = st.session_state['current_generated_usecases']
        usecase_idx = 0
        for usecase in usecases:
            st.markdown("## "+usecase['pipeline_name'])
            st.markdown(usecase['description'])
            add_usecase_btn = st.button("Apply to pipeline", key=f"add_usecase_{usecase_idx}")
            usecase_idx += 1
            if add_usecase_btn and "current_pipeline_obj" in st.session_state:
                st.session_state["current_pipeline_obj"]["name"] = usecase['pipeline_name'].replace(" ", "_")
                st.session_state["current_pipeline_obj"]["description"] = usecase['description']
                st.experimental_rerun()

            st.divider()


with tab_data:
# Initialize current_generated_tables key in session state
    if "current_generated_tables" not in st.session_state:
        st.session_state['current_generated_tables'] = {}
    current_generated_tables = st.session_state['current_generated_tables']

    # Get industry name from global pipeline config object
    industry_name = st.session_state["current_pipeline_obj"].get("industry", "")
    industry_contexts = st.session_state["current_pipeline_obj"].get("description", "")

    # Initialize generated sample data key in session state
    if "current_generated_sample_data" not in st.session_state:
        st.session_state['current_generated_sample_data'] = {}
    current_generated_sample_data = st.session_state['current_generated_sample_data']
    
    if "disable_generate_table_button" not in st.session_state:
        st.session_state["disable_generate_table_button"] = False

    # AI Assistnt for tables generation
    tables = []
    placeholder = st.empty()
    container = placeholder.container()
    with container:
        st.button("Generate",
                  on_click=streamlit_utils.generate_tables,
                  args=[placeholder,
                        current_generated_tables,
                        current_pipeline_obj,
                        current_generated_sample_data,
                        industry_name,
                        industry_contexts,
                        openai_api],
                  disabled=st.session_state["disable_generate_table_button"],
                  use_container_width=True)

    # Show warning message if some of generated tables have been referenced by other std/srv tasks
    if streamlit_utils.has_staged_table() and st.session_state["has_clicked_generate_tables_btn"]:
        with container:
            st.warning("Some of current generated tables have been added to Staging zone, please remove them before generating tables again!")
        st.session_state["has_clicked_generate_tables_btn"] = False
        st.session_state["disable_generate_data_widget"] = False

    # Render generated tables once widgets inside table expander has been clicked/updated
    if "generated_tables" in current_generated_tables and not st.session_state["has_clicked_generate_tables_btn"]:
        tables = current_generated_tables["generated_tables"]
        with container:
            for gen_table_index, table in enumerate(tables):
                streamlit_utils.render_table_expander(table,
                                                    current_generated_tables,
                                                    current_generated_sample_data,
                                                    current_pipeline_obj,
                                                    gen_table_index,
                                                    industry_name,
                                                    openai_api)
    elif "generate_tables_count" in st.session_state and len(current_generated_tables.get("generated_tables", [])) == st.session_state["generate_tables_count"]:
        st.session_state["disable_generate_data_widget"] = False
        tables = current_generated_tables["generated_tables"]

        with placeholder.container():
            st.button("Generate",
                      key="generate_table_redraw",
                      on_click=streamlit_utils.generate_tables,
                      args=[placeholder,
                            current_generated_tables,
                            current_pipeline_obj,
                            current_generated_sample_data,
                            industry_name,
                            industry_contexts,
                            openai_api],
                      disabled=st.session_state["disable_generate_table_button"],
                      use_container_width=True)

            for gen_table_index, table in enumerate(tables):
                streamlit_utils.render_table_expander(table,
                                                    current_generated_tables,
                                                    current_generated_sample_data,
                                                    current_pipeline_obj,
                                                    gen_table_index,
                                                    industry_name,
                                                    openai_api,
                                                    "redraw")


with tab_std_sql:
    if "current_editing_pipeline_tasks" not in st.session_state:
        st.session_state['current_editing_pipeline_tasks'] = {}
    current_editing_pipeline_tasks = st.session_state['current_editing_pipeline_tasks']
    if "standard" not in current_editing_pipeline_tasks:
        current_editing_pipeline_tasks['standard'] = []

    # Get all staged table details
    staged_table_names, staged_table_details = streamlit_utils.get_staged_tables()

    for i in range(len(current_pipeline_obj["standard"])):
        target_name = current_pipeline_obj['standard'][i]['output']['target']
        disable_std_name_input = False
        disable_std_task_deletion = False
        std_name_has_dependency = streamlit_utils.check_tables_dependency(target_name)

        if std_name_has_dependency:
            disable_std_name_input = True
            disable_std_task_deletion = True
            st.info("""This standardization task has been referenced by other tasks!
                    Please remove relevant dependency before trying to rename or delete this task.""")

        std_name = st.text_input('Transformation Name',
                                 key=f'std_{i}_name',
                                 value=target_name,
                                 disabled=disable_std_name_input)

        if std_name:
            st.subheader(std_name)
            with st.expander(std_name+" Settings", expanded=True):
                current_pipeline_obj['standard'][i]['output']['target'] = std_name
                current_pipeline_obj['standard'][i]['name'] = std_name

                # Get latest standardized table details
                standardized_table_names, standardized_table_details = streamlit_utils.get_std_srv_tables('standard')

                optional_tables = staged_table_names + standardized_table_names
                if std_name in optional_tables:
                    optional_tables.remove(std_name)    # Remove itself from the optional tables list

                if len(current_editing_pipeline_tasks['standard']) == i:
                    current_editing_pipeline_tasks['standard'].append({})
                current_editing_pipeline_tasks['standard'][i]['target'] = std_name
                selected_staged_tables = st.multiselect(
                    'Choose datasets to do the data transformation',
                    options=optional_tables,
                    default=current_editing_pipeline_tasks['standard'][i].get('involved_tables', None),
                    on_change=streamlit_utils.update_selected_tables,
                    key=f'std_{i}_involved_tables',
                    args=['standard', i, f'std_{i}_involved_tables'])

                if 'description' not in current_pipeline_obj['standard'][i]:
                    current_pipeline_obj['standard'][i]['description'] = ""
                process_requirements = st.text_area("Transformation requirements",
                                                    key=f"std_{i}_transform_requirements",
                                                    value=current_pipeline_obj['standard'][i]['description'])
                current_pipeline_obj['standard'][i]['description'] = process_requirements

                generate_transform_sql_col1, generate_transform_sql_col2 = st.columns(2)
                with generate_transform_sql_col1:
                    st.button(f'Generate SQL',
                            key=f'std_{i}_gen_sql',
                            on_click=streamlit_utils.click_button,
                            kwargs={"button_name": f"std_{i}_gen_transform_sql"})

                if len(current_pipeline_obj['standard'][i]['code']['sql']) == 0:
                    current_pipeline_obj['standard'][i]['code']['sql'].append("")
                std_sql_val = current_pipeline_obj['standard'][i]['code']['sql'][0]

                # Get selected staged table details
                selected_table_details = streamlit_utils.get_selected_tables_details(staged_table_details + standardized_table_details,
                                                                                     selected_staged_tables)
                if st.session_state[f"std_{i}_gen_sql"]:
                    with generate_transform_sql_col2:
                        with st.spinner('Generating...'):
                            try:
                                process_logic = openai_api.generate_custom_data_processing_logics(industry_name=industry_name,
                                                                                                industry_contexts=industry_contexts,
                                                                                                involved_tables=selected_table_details,
                                                                                                custom_data_processing_logic=process_requirements,
                                                                                                output_table_name=std_name)

                                process_logic_json = json.loads(process_logic)
                                std_sql_val = process_logic_json["sql"]
                                std_table_schema = process_logic_json["schema"]
                                current_editing_pipeline_tasks['standard'][i]['query_results_schema'] = std_table_schema
                                current_pipeline_obj['standard'][i]['code']['sql'][0] = std_sql_val
                            except ValueError as e:
                                st.error("Got invalid response from AI Assistant, please try again!")
                            except Exception as e:
                                st.error("Got error while getting help from AI Assistant, please try again!")

                std_sql = st.text_area(f'Transformation Spark SQL',
                                        key=f'std_{i}_transform_sql_text_area',
                                        value=std_sql_val)

                current_pipeline_obj['standard'][i]['code']['sql'][0] = std_sql

                st.button(f'Run SQL', key=f'run_std_{i}_sql', on_click=streamlit_utils.run_task, args = [std_name, "standard", i])
                if 'sql_query_results' in current_editing_pipeline_tasks['standard'][i]:
                    st.dataframe(current_editing_pipeline_tasks['standard'][i]['sql_query_results'])

            st.button(f"Delete {std_name}",
                      key="delete_std_"+str(i),
                      on_click=streamlit_utils.delete_task,
                      args = ['standard', i],
                      disabled=disable_std_task_deletion)

        if i != len(current_pipeline_obj["standard"]) - 1:
            st.divider()

    if len(current_pipeline_obj["standard"]) == 0:
        st.write("No transformation in standardization zone")

    st.divider()
    st.button('Add Transformation', on_click=streamlit_utils.add_transformation, use_container_width=True)


with tab_srv_sql:

    if "serving" not in current_editing_pipeline_tasks:
        current_editing_pipeline_tasks['serving'] = []

    for i in range(len(current_pipeline_obj["serving"])):
        target_name = current_pipeline_obj['serving'][i]['output']['target']
        srv_name_has_dependency = streamlit_utils.check_tables_dependency(target_name)
        disable_srv_name_input = False
        disable_srv_task_deletion = False

        if srv_name_has_dependency:
            disable_srv_name_input = True
            disable_srv_task_deletion = True
            st.info("""This serving task has been referenced by other tasks!
                    Please remove relevant dependency before trying to rename the task.""")

        srv_name = st.text_input('Aggregation Name',
                                 key=f'srv_{i}_name',
                                 value=target_name,
                                 disabled=srv_name_has_dependency)

        if srv_name:
            st.subheader(srv_name)
            with st.expander(srv_name+" Settings", expanded=True):
                current_pipeline_obj['serving'][i]['output']['target'] = srv_name
                current_pipeline_obj['serving'][i]['name'] = srv_name

                # Get all standardized table details
                serving_table_names, serving_table_details = streamlit_utils.get_std_srv_tables('serving')
                optional_tables = staged_table_names + standardized_table_names + serving_table_names
                if srv_name in optional_tables:
                    optional_tables.remove(srv_name)    # Remove itself from the optional tables list

                if len(current_editing_pipeline_tasks['serving']) == i:
                    current_editing_pipeline_tasks['serving'].append({})
                current_editing_pipeline_tasks['serving'][i]['target'] = srv_name

                selected_staged_tables = st.multiselect(
                    'Choose datasets to do the data aggregation',
                    options=optional_tables,
                    default=current_editing_pipeline_tasks['serving'][i].get('involved_tables', None),
                    on_change=streamlit_utils.update_selected_tables,
                    key=f'srv_{i}_involved_tables',
                    args=['serving', i, f'srv_{i}_involved_tables'])

                if 'description' not in current_pipeline_obj['serving'][i]:
                    current_pipeline_obj['serving'][i]['description'] = ""
                process_requirements = st.text_area("Aggregation requirements",
                                                    key=f"srv_{i}_aggregate_requirements",
                                                    value=current_pipeline_obj['serving'][i]['description'])
                current_pipeline_obj['serving'][i]['description'] = process_requirements

                generate_aggregate_sql_col1, generate_aggregate_sql_col2 = st.columns(2)
                with generate_aggregate_sql_col1:
                    st.button(f'Generate SQL',
                            key=f'srv_{i}_gen_sql',
                            on_click=streamlit_utils.click_button,
                            kwargs={"button_name": f"srv_{i}_gen_aggregate_sql"})

                if len(current_pipeline_obj['serving'][i]['code']['sql']) == 0:
                    current_pipeline_obj['serving'][i]['code']['sql'].append("")
                srv_sql_val = current_pipeline_obj['serving'][i]['code']['sql'][0]

                # Get selected staged table details
                selected_table_details = streamlit_utils.get_selected_tables_details(staged_table_details + standardized_table_details + serving_table_details,
                                                                                     selected_staged_tables)
                if st.session_state[f"srv_{i}_gen_sql"]:
                    with generate_aggregate_sql_col2:
                        with st.spinner('Generating...'):
                            try:
                                process_logic = openai_api.generate_custom_data_processing_logics(industry_name=industry_name,
                                                                                                industry_contexts=industry_contexts,
                                                                                                involved_tables=selected_table_details,
                                                                                                custom_data_processing_logic=process_requirements,
                                                                                                output_table_name=srv_name)

                                process_logic_json = json.loads(process_logic)
                                srv_sql_val = process_logic_json["sql"]
                                srv_table_schema = process_logic_json["schema"]
                                current_editing_pipeline_tasks['serving'][i]['query_results_schema'] = srv_table_schema
                                current_pipeline_obj['serving'][i]['code']['sql'][0] = srv_sql_val
                            except ValueError as e:
                                st.error("Got invalid response from AI Assistant, please try again!")
                            except Exception as e:
                                st.error("Got error while getting help from AI Assistant, please try again!")

                srv_sql = st.text_area(f'Aggregation Spark SQL',
                                        key=f'srv_{i}_aggregate_sql_text_area',
                                        value=srv_sql_val)

                current_pipeline_obj['serving'][i]['code']['sql'][0] = srv_sql

                st.button(f'Run SQL', key=f'run_srv_{i}_sql', on_click=streamlit_utils.run_task, args = [srv_name, "serving", i])
                if 'sql_query_results' in current_editing_pipeline_tasks['serving'][i]:
                    st.dataframe(current_editing_pipeline_tasks['serving'][i]['sql_query_results'])

            st.button(f"Delete {srv_name}",
                      key="delete_srv_"+str(i),
                      on_click=streamlit_utils.delete_task,
                      args = ['serving', i],
                      disabled=disable_srv_task_deletion)

        if i != len(current_pipeline_obj["serving"]) - 1:
            st.divider()

    if len(current_pipeline_obj["serving"]) == 0:
        st.write("No aggregation in serving zone")

    st.divider()
    st.button('Add Aggregation', on_click=streamlit_utils.add_aggregation, use_container_width=True)
