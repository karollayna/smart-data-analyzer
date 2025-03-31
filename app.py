import streamlit as st
import pandas as pd
import backend
import plotly.express as px

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

if "data_uploaded" not in st.session_state:
    st.session_state["data_uploaded"] = False
    st.session_state['parameters_for_plot_selected'] = False
    st.session_state['plot_created'] = False
    st.session_state["selected_x_axis"] = None
    st.session_state["selected_y_axis"] = None
    st.session_state["selected_additional_parameter"] = None

if not st.session_state["data_uploaded"]:
    uploaded_files = backend.upload_user_files()
    if uploaded_files:
        valid_files = backend.validate_user_data(uploaded_files)
        if valid_files:
            if st.button("Save your data :cloud:"):
                with st.spinner("Uploading your data to the cloud..."):
                    uploaded_files = backend.upload_files_to_s3(valid_files)
                    if uploaded_files:
                        st.success(
                            ":white_check_mark: Your data has been saved to the cloud."
                        )
                        st.session_state["data_uploaded"] = True
                    
if st.session_state["data_uploaded"] and not st.session_state['parameters_for_plot_selected']:
    if st.button("Show your data :bar_chart:"):
        with st.spinner("Loading your data..."):
            df = backend.connect_with_snowflake()
            st.success(":white_check_mark: Your data has been loaded successfully.")
        
            st.session_state['parameters_for_plot_selected'] = True
            st.session_state['df'] = df
            st.session_state["selected_x_axis"] = df["DRUG_CONCENTRATION"]
            st.session_state['selected_y_axis'] = df["SURVIVAL_RATE_PERCENT"]
            st.session_state['selected_additional_parameter'] = df["CELL_LINE_NAME"]

if st.session_state['parameters_for_plot_selected']:
    if st.button("Create your plot :bar_chart:"):
        with st.spinner("Creating your plot..."):
            fig = px.scatter(
                st.session_state['df'],
                x=st.session_state["selected_x_axis"],
                y=st.session_state["selected_y_axis"],
                color=st.session_state["selected_additional_parameter"]
            )
     
            st.plotly_chart(fig)
            st.session_state['plot_created'] = True
