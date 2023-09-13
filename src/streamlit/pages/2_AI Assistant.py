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
    label="AI Assiatant",
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
        with generate_tables_col2:
            with st.spinner('Generating...'):
                tables = openai_api.recommend_tables_for_industry(industry_name, industry_contexts)
                current_generated_tables["generated_tables"] = tables

    try:
        if "generated_tables" in current_generated_tables:
            tables = json.loads(current_generated_tables["generated_tables"])

        if "selected_tables" not in current_generated_tables:
            current_generated_tables["selected_tables"] = []

        for table in tables:
            columns = table["columns"]
            columns_df = pd.DataFrame.from_dict(columns, orient='columns')

            check_flag = False
            for selected_table in current_generated_tables["selected_tables"]:
                if selected_table == table["table_name"]:
                    check_flag = True

            sample_data = None
            with st.expander(table["table_name"]):
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
                                if sample_data and st.session_state[gen_table_name]:
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

                    st.checkbox("Add to staging zone",
                        key=gen_table_name,
                        value=check_flag,
                        on_change=streamlit_utils.add_to_staging_zone,
                        args=[gen_table_name, gen_table_desc])


            # TODO we need to change the key of table["table_name"]
            if st.session_state[table["table_name"]] and table["table_name"] not in current_generated_tables["selected_tables"]:
                current_generated_tables["selected_tables"].append(table["table_name"])

            
                
    except ValueError as e:
        # TODO: Add error/exception to standard error-showing widget
        st.write(tables)

with tab_std_sql:

    # st.divider()
    # st.subheader('Generate data transformation logic')


    if "standardized_tables" not in st.session_state:
        st.session_state["standardized_tables"] = []
    standardized_tables = st.session_state["standardized_tables"]

    if "current_generated_std_srv_sqls" not in st.session_state:
        st.session_state['current_generated_std_srv_sqls'] = {}
    current_generated_std_srv_sqls = st.session_state['current_generated_std_srv_sqls']

    # Get all staged table details
    staged_table_names, staged_table_details = streamlit_utils.get_staged_tables()
    # Get all standardized table details
    standardized_table_names, standardized_table_details = streamlit_utils.get_standardized_tables()

    std_name = st.text_input("Transformation Name")
    selected_staged_tables = st.multiselect(
        'Choose datasets to do the data transformation',
        staged_table_names + standardized_table_names,
        key=f'std_involved_tables')
    process_requirements = st.text_area("Transformation requirements", key=f"std_transform_requirements")

    generate_transform_sql_col1, generate_transform_sql_col2 = st.columns(2)
    with generate_transform_sql_col1:
        st.button(f'Generate SQL',
                key=f'std_gen_sql',
                on_click=streamlit_utils.click_button,
                kwargs={"button_name": f"std_gen_transform_sql"})

    if "std_gen_transform_sql" not in st.session_state:
        st.session_state["std_gen_transform_sql"] = False
    if st.session_state["std_gen_transform_sql"]:
        st.session_state["std_gen_transform_sql"] = False    # Reset clicked status
        with generate_transform_sql_col2:
            with st.spinner('Generating...'):
                process_logic = openai_api.generate_custom_data_processing_logics(industry_name=industry_name,
                                                                                industry_contexts=industry_contexts,
                                                                                involved_tables=staged_table_details + standardized_table_details,
                                                                                custom_data_processing_logic=process_requirements,
                                                                                output_table_name=std_name)
                try:
                    process_logic_json = json.loads(process_logic)
                    std_sql_val = process_logic_json["sql"]
                    current_generated_std_srv_sqls[std_name] = std_sql_val
                except ValueError as e:
                    st.write(process_logic)

        with st.expander(std_name, expanded=True):
            st.button("Add to Standardization zone",
                        key=f"gen_transformation_{std_name}",
                        on_click=streamlit_utils.add_to_std_srv_zone,
                        args=[f"gen_transformation_{std_name}", std_name, process_requirements, "standard"])
            st.text_input("Invovled tables", value=", ".join(selected_staged_tables), disabled=True)
            std_sql = st.text_area(f'Transformation Spark SQL',
                                    key=f'std_transform_sql_{std_name}',
                                    value=current_generated_std_srv_sqls[std_name],
                                    on_change=streamlit_utils.update_sql,
                                    args=[f'std_transform_sql_{std_name}', std_name])


with tab_srv_sql:
    # st.divider()
    # st.subheader('Generate data aggregation logic')

    if "serving_tables" not in st.session_state:
        st.session_state["serving_tables"] = []
    serving_tables = st.session_state["serving_tables"]

    # Get all standardized table details
    standardized_table_names, standardized_table_details = streamlit_utils.get_standardized_tables()

    srv_name = st.text_input("Aggregation Name")
    selected_stg_std_tables = st.multiselect(
        'Choose datasets to do the data aggregation',
        staged_table_names + standardized_table_names,
        key=f'srv_involved_tables')
    process_requirements = st.text_area("Aggregation requirements", key=f"srv_aggregate_requirements")

    generate_aggregate_sql_col1, generate_aggregate_sql_col2 = st.columns(2)
    with generate_aggregate_sql_col1:
        st.button(f'Generate SQL',
                key=f'srv_gen_sql',
                on_click=streamlit_utils.click_button,
                kwargs={"button_name": f"srv_gen_aggregate_sql"})

    if "srv_gen_aggregate_sql" not in st.session_state:
        st.session_state["srv_gen_aggregate_sql"] = False
    if st.session_state["srv_gen_aggregate_sql"]:
        st.session_state["srv_gen_aggregate_sql"] = False    # Reset clicked status
        with generate_aggregate_sql_col2:
            with st.spinner('Generating...'):
                process_logic = openai_api.generate_custom_data_processing_logics(industry_name=industry_name,
                                                                                industry_contexts=industry_contexts,
                                                                                involved_tables=staged_table_details,
                                                                                custom_data_processing_logic=process_requirements,
                                                                                output_table_name=srv_name)
                try:
                    process_logic_json = json.loads(process_logic)
                    srv_sql_val = process_logic_json["sql"]
                    current_generated_std_srv_sqls[srv_name] = srv_sql_val
                except ValueError as e:
                    st.write(process_logic)

        with st.expander(srv_name, expanded=True):
            st.button("Add to Serving zone",
                        key=f"gen_aggregation_{srv_name}",
                        on_click=streamlit_utils.add_to_std_srv_zone,
                        args=[f"gen_aggregation_{srv_name}", srv_name, process_requirements, "serving"])
            st.text_input("Invovled tables", value=", ".join(selected_stg_std_tables), disabled=True)
            std_sql = st.text_area(f'Transformation Spark SQL',
                                    key=f'srv_aggregate_sql_{srv_name}',
                                    value=current_generated_std_srv_sqls[srv_name],
                                    on_change=streamlit_utils.update_sql,
                                    args=[f'std_transform_sql_{srv_name}', srv_name])