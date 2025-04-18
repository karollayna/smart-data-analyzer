import streamlit as st
from data_handler import DataHandler
from aws.aws_handler import AWSHandler
from snow.snow_handler import SnowflakeHandler
import uuid
import pandas as pd

st.title(
    """
Smart Data Analyzer :female-scientist: :male-scientist:
:dart: This page makes it easy for you to create graphs from your data
"""
)
st.markdown(
    "Your data should be in the following **format**:",
    help="""
📋 **Expected file format:** `.csv`
📋 **Required files and columns:**

**`data_photodynamic_therapy_cell_lines.csv`**
- `cell_line_code`
- `cell_line_name`

**`data_photodynamic_therapy_drugs.csv`**
- `drug_code`
- `drug_name`

**`data_photodynamic_therapy_results.csv`**
- `experiment_id`
- `experiment_number`
- `cell_line_code`
- `treatment_time`
- `drug_code`
- `drug_concentration`
- `result_001 - result_012`

**Note:** The number of `result` columns is fixed at 12.
""",
)

data_handler = DataHandler()
aws_handler = AWSHandler()
snow_handler = SnowflakeHandler() 


if "data_uploaded" not in st.session_state:
    st.session_state['user_id'] = None
    st.session_state["data_uploaded"] = False
    st.session_state["snowflake_connected"] = False
    st.session_state["data_updated"] = False
    st.session_state["data_analyzed"] = False
    st.session_state['data'] = {}
    st.session_state['plot_created'] = False

if st.session_state['user_id'] is None:
    ##TODO: user can choose if they want to generate a new ID or use an existing one
    ##TODO: create a button to generate a new ID
    ##TODO: create a button to use an existing ID
    ##TODO: create a function to check if the ID already exists in the database
    # generate a unique user ID
    full_uuid = uuid.uuid4()
    hex_uuid = full_uuid.hex
    st.session_state['user_id'] = hex_uuid[:10]

with st.container():
    ##TODO: add note to inform the user that the ID is generated automatically or that they can choose an existing one
    st.write(f"**Your Unique ID:** {st.session_state['user_id']}")

if not st.session_state["data_uploaded"]:
    ##TODO: add function to this part
    uploaded_files = data_handler.upload_user_files()
    if uploaded_files:
        valid_files = data_handler.validate_user_data(uploaded_files)
        if valid_files:
            if st.button("Save your data :cloud:"):
                with st.spinner("Uploading your data to the cloud..."):
                    uploaded_files = aws_handler.upload_files_to_s3(valid_files)
                    if uploaded_files:
                        st.success(
                            ":white_check_mark: Your data has been saved to the cloud."
                        )
                        st.session_state["data_uploaded"] = True 

                   
if st.session_state["data_uploaded"] and not st.session_state['snowflake_connected']:
    with st.spinner('Waiting for Snowflake...', show_time=True):
        st.session_state['snowflake_connected'] = True

        snow_handler.reset_pipeline()

if st.session_state['snowflake_connected'] and not st.session_state['data_updated']:
    ##TODO: find a faster way to do this
    with st.spinner("Merging data into target tables..."):
        snow_handler.call_procedure("merge_into_dim_cell_lines()")
        snow_handler.call_procedure("merge_into_dim_drugs()")
        snow_handler.call_procedure("merge_into_fac_results()")

    tables = ["dim_cell_lines", "dim_drugs", "fac_results"]
    for table in tables:
        with st.spinner(f'Fetching {table}...'):
            columns, data = snow_handler.fetch_data(table)
            df = pd.DataFrame(data, columns=columns)
            st.session_state["data"][table] = df

    results = snow_handler.fetch_full_data("combined_results", st.session_state['user_id'])
    st.session_state['data'] = results
    st.session_state["data_updated"] = True

if st.session_state['data_updated'] and not st.session_state['data_analyzed']:
    ##TODO: write test for this part
    df = st.session_state['data']
    number = st.number_input("Insert experiment number", value = None, step = 1, min_value = 1)

    if number is not None:
        with st.spinner('Analyzing your data...'):
            experiment_data = data_handler.fetch_experiment_data(data = df, experiment_number = number)
            experiment_data_analyzed, control_means, full_experiment_data = data_handler.analyze_experiment_data(experiment_data)
            user_result = data_handler.calculate_survival(full_experiment_data)

            with st.expander("Analysis Results:"):
                st.write(user_result)

            st.subheader("Select parameters for your plot")
            
            options = list(user_result.columns)
            cell_lines = user_result['CELL_LINE_NAME'].unique()
            drugs = user_result['DRUG_NAME'].unique()
            treatment_times = user_result['TREATMENT_TIME'].unique()

        col1, col2, col3 = st.columns(3)
        with col1:
            x_axis = st.selectbox("Select X-axis:", options, index=options.index('DRUG_CONCENTRATION'))
        with col2:
            y_axis = st.selectbox("Select Y-axis:", options, index=options.index('SURVIVAL_RATE'))
        with col3:
            filter_type = st.radio("Filter by:", ["Drugs", "Cell Lines"], index=0, key="filter_type")
            
        if filter_type == "Drugs":
            selected_value = st.selectbox("Select drug:", drugs)
        else:
            selected_value = st.selectbox("Select cell line:", cell_lines) 
        
        ##TODO: create plots: interactive and publication ready
        if st.button("Create your plot :bar_chart:"):
            with st.spinner("Creating your plot..."):
                figures = data_handler.create_plots(user_result, filter_type, selected_value, x_axis, y_axis, treatment_times)
                for fig in figures:
                    st.plotly_chart(fig)
                st.session_state['plot_created'] = True

        ##TODO: download plots as a report pdf

if snow_handler:
    snow_handler.close_connection()