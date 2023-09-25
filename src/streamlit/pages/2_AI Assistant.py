import cddp
import json
import os
import pandas as pd
import streamlit as st
import sys
import streamlit_utils
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
import cddp.openai_api as openai_api
from streamlit_extras.switch_page_button import switch_page
from streamlit_extras.colored_header import colored_header

if "current_pipeline_obj" not in st.session_state:
    switch_page("Home")

current_pipeline_obj = st.session_state["current_pipeline_obj"]


st.set_page_config(page_title="AI Assistant")

colored_header(
    label="AI Assistant",
    description=f"Leverage AI to assist you in data pipeline development",
    color_name="violet-70",
)




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
                usecases = openai_api.recommend_data_processing_scenario(current_pipeline_obj.get("industry", ""))
                st.session_state['current_generated_usecases'] = json.loads(usecases)

    
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
# AI Assistnt for tables generation
    tables = []
    generate_tables_col1, generate_tables_col2 = st.columns(2)
    with generate_tables_col1:
        st.button("Generate", on_click=streamlit_utils.click_button, kwargs={"button_name": "generate_tables"}, use_container_width=True)

    if "generate_tables" not in st.session_state:
        st.session_state["generate_tables"] = False
    elif st.session_state["generate_tables"]:
        st.session_state["generate_tables"] = False     # Reset button clicked status

        has_staged_table = streamlit_utils.has_staged_table()

        if has_staged_table:
            st.warning("Some of current generated tables have been added to Staging zone, please remove them before generating tables again!")
        else:
            with generate_tables_col2:
                with st.spinner('Generating...'):
                    tables = openai_api.recommend_tables_for_industry(industry_name, industry_contexts)
                    current_generated_tables["generated_tables"] = json.loads(tables)

                    # Clean current_generated_sample_data key in session state once Generate button is clicked again
                    st.session_state['current_generated_sample_data'] = {}

    try:
        if "generated_tables" in current_generated_tables:
            tables = current_generated_tables["generated_tables"]

        for gen_table_index, table in enumerate(tables):
            columns = table["columns"]
            columns_df = pd.DataFrame.from_dict(columns, orient='columns')

            sample_data = None
            added_to_stage = current_generated_tables["generated_tables"][gen_table_index].get("staged_flag", False)
            expander_label = table["table_name"]
            if added_to_stage:
                expander_label = table["table_name"] + "     :heavy_check_mark:"

            with st.expander(expander_label, expanded=added_to_stage):
                gen_table_name = table["table_name"]
                gen_table_desc = table["table_description"]

                st.write(gen_table_desc)
                st.write(columns_df)

                st.write(f"Generate sample data")
                rows_count = st.slider("Number of rows", min_value=5, max_value=50, key=f'gen_rows_count_slider_{gen_table_name}')
                enable_data_requirements = st.toggle("With extra sample data requirements", key=f'data_requirements_toggle_{gen_table_name}')
                data_requirements = ""
                if enable_data_requirements:
                    data_requirements = st.text_area("Extra requirements for sample data",
                                                    key=f'data_requirements_text_area_{gen_table_name}',
                                                    placeholder="Exp: value of column X should follow patterns xxx-xxxx, while x could be A-Z or 0-9")

                generate_sample_data_col1, generate_sample_data_col2 = st.columns(2)
                with generate_sample_data_col1:
                    st.button("Generate Sample Data",
                            key=f"generate_data_button_{gen_table_name}",
                            on_click=streamlit_utils.click_button,
                            kwargs={"button_name": f"generate_sample_data_{gen_table_name}"})

                if f"generate_sample_data_{gen_table_name}" not in st.session_state:
                    st.session_state[f"generate_sample_data_{gen_table_name}"] = False

                if f"{gen_table_name}_smaple_data_generated" not in st.session_state:
                    st.session_state[f"{gen_table_name}_smaple_data_generated"] = False
                elif st.session_state[f"generate_sample_data_{gen_table_name}"]:
                    st.session_state[f"generate_sample_data_{gen_table_name}"] = False      # Reset clicked status
                    if not st.session_state[f"{gen_table_name}_smaple_data_generated"]:
                        with generate_sample_data_col2:
                            with st.spinner('Generating...'):
                                sample_data = openai_api.generate_sample_data(industry_name, 
                                                                                rows_count,
                                                                                table,
                                                                                data_requirements)
                                st.session_state[f"{gen_table_name}_smaple_data_generated"] = True
                                # Store generated data to session_state
                                current_generated_sample_data[gen_table_name] = sample_data

                                # Also update current_pipeline_obj if checked check-box before generating sample data
                                # if sample_data and st.session_state[f"add_to_staging_{gen_table_index}_checkbox"]:
                                if sample_data and st.session_state.get(f"add_to_staging_{gen_table_index}_checkbox", False):
                                    spark = st.session_state["spark"]
                                    json_str, schema = cddp.load_sample_data(spark, sample_data, format="json")

                                    for index, dataset in enumerate(current_pipeline_obj['staging']):
                                        if dataset["name"] == gen_table_name:
                                            i = index

                                    current_pipeline_obj['staging'][i]['sampleData'] = json.loads(json_str)
                                    current_pipeline_obj['staging'][i]['schema'] = json.loads(schema)

                    if st.session_state[f"{gen_table_name}_smaple_data_generated"]:
                        st.session_state[f"{gen_table_name}_smaple_data_generated"] = False     # Reset data generated flag
                        json_sample_data = json.loads(sample_data)
                        current_generated_sample_data[gen_table_name] = json_sample_data      # Save generated data to session_state
                        # st.session_state[f'stg_{i}_data'] = sample_data

                if gen_table_name in current_generated_sample_data:
                    sample_data_df = pd.DataFrame.from_dict(current_generated_sample_data[gen_table_name], orient='columns')
                    st.write(sample_data_df)

                    # Show checkbox only after sample data has been generated 
                    st.checkbox("Add to staging zone",
                        key=f"add_to_staging_{gen_table_index}_checkbox",
                        value=added_to_stage,
                        on_change=streamlit_utils.add_to_staging_zone,
                        args=[gen_table_index, gen_table_name, gen_table_desc])
                
    except ValueError as e:
        # TODO: Add error/exception to standard error-showing widget
        st.write(tables)

with tab_std_sql:
    if "current_editing_pipeline_tasks" not in st.session_state:
        st.session_state['current_editing_pipeline_tasks'] = {}
    current_editing_pipeline_tasks = st.session_state['current_editing_pipeline_tasks']
    if "standard" not in current_editing_pipeline_tasks:
        current_editing_pipeline_tasks['standard'] = []

    # Get all staged table details
    staged_table_names, staged_table_details = streamlit_utils.get_staged_tables()

    # Get all standardized table details
    standardized_table_names, standardized_table_details = streamlit_utils.get_std_srv_tables('standard')

    for i in range(len(current_pipeline_obj["standard"])):
        target_name = current_pipeline_obj['standard'][i]['output']['target']
        disable_std_name_input = False
        disable_std_task_deletion = False
        std_name_has_dependency = streamlit_utils.check_tables_dependency(target_name)

        if std_name_has_dependency:
            disable_std_name_input = True
            disable_std_task_deletion = True
            st.info("""This standardization task has been referenced in other tasks!
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
                            process_logic = openai_api.generate_custom_data_processing_logics(industry_name=industry_name,
                                                                                            industry_contexts=industry_contexts,
                                                                                            involved_tables=selected_table_details,
                                                                                            custom_data_processing_logic=process_requirements,
                                                                                            output_table_name=std_name)
                            try:
                                process_logic_json = json.loads(process_logic)
                                std_sql_val = process_logic_json["sql"]
                                std_table_schema = process_logic_json["schema"]
                                current_editing_pipeline_tasks['standard'][i]['query_results_schema'] = std_table_schema
                                current_pipeline_obj['standard'][i]['code']['sql'][0] = std_sql_val
                            except ValueError as e:
                                st.write(process_logic)

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
            st.info("""This serving task has been referenced in other tasks!
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
                            process_logic = openai_api.generate_custom_data_processing_logics(industry_name=industry_name,
                                                                                            industry_contexts=industry_contexts,
                                                                                            involved_tables=selected_table_details,
                                                                                            custom_data_processing_logic=process_requirements,
                                                                                            output_table_name=srv_name)
                            try:
                                process_logic_json = json.loads(process_logic)
                                srv_sql_val = process_logic_json["sql"]
                                srv_table_schema = process_logic_json["schema"]
                                current_editing_pipeline_tasks['serving'][i]['query_results_schema'] = srv_table_schema
                                current_pipeline_obj['serving'][i]['code']['sql'][0] = srv_sql_val
                            except ValueError as e:
                                st.write(process_logic)

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
