import streamlit as st
import pandas as pd
import backend
import uuid
import plotly.express as px
import time

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
    st.session_state['user_id'] = None
    st.session_state["data_uploaded"] = False
    st.session_state["snowflake_connected"] = False
    st.session_state["data_updated"] = False
    st.session_state['data'] = {}
    st.session_state['parameters_for_plot_selected'] = False
    st.session_state['plot_created'] = False

if st.session_state['user_id'] is None:
    full_uuid = uuid.uuid4()
    hex_uuid = full_uuid.hex
    st.session_state['user_id'] = hex_uuid[:10]

with st.container():
    st.write(f"**Your Unique ID:** {st.session_state['user_id']}")
    st.markdown(f"<h2 style='background-color:lightblue; border:1px solid black; padding:5px; border-radius:5px;'>{st.session_state['user_id']}</h2>", unsafe_allow_html=True)
    st.write("Please save this ID as it will be needed later in the application.")

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
                    
if st.session_state["data_uploaded"] and not st.session_state['snowflake_connected']:
    with st.spinner('Connecting with Snowflake...'):
        conn = backend.connect_with_snowflake()
        st.session_state['snowflake_connected'] = True

if st.session_state['snowflake_connected'] and not st.session_state['data_updated']:
    tables_pipes = {
        'dim_cell_lines': 'update_dim_cell_lines',
        'dim_drugs': 'update_dim_drugs',
        'fac_results': 'update_fac_results',
    }

    for table, pipe in tables_pipes.items():
        with st.spinner(f'Refreshing data {table}', show_time=True):
            backend.refresh_snowpipe(pipe)
            time.sleep(15) 
            columns, data = backend.fetch_data(table)
            users_data = pd.DataFrame(data, columns=columns)

            st.session_state["data"][table] = users_data
            st.session_state["data_updated"] = True
            st.success(f"Data from {table} updated!")

    fac_results = st.session_state["data"]["fac_results"]
    cell_lines = st.session_state["data"]["dim_cell_lines"]
    drugs = st.session_state["data"]["dim_drugs"]

    results = backend.fetch_full_data("combined_results", st.session_state['user_id'])
    st.session_state['data'] = results
    st.write(results)

if st.session_state['data_updated'] and not st.session_state['parameters_for_plot_selected']:
    st.subheader("Select parameters for your plot")
    df = st.session_state['data']
    options = list(df.columns)
    cell_lines = df['CELL_LINE_NAME'].str.lower().unique().tolist()
    drugs = df['DRUG_NAME'].str.lower().unique().tolist()

    col1, col2, col3 = st.columns(3)
    with col1:
        x_axis = st.selectbox("Select X-axis:", options, index=options.index('DRUG_CONCENTRATION'))
    with col2:
        y_axis = st.selectbox("Select Y-axis:", options, index=options.index('SURVIVAL_RATE_PERCENT'))
    with col3:
        filter_type = st.radio("Filter by:", ["Drugs", "Cell Lines"], index=0, key="filter_type")
        
    if filter_type == "Drugs":
        selected_value = st.selectbox("Select drug:", drugs)
    else:
        selected_value = st.selectbox("Select cell line:", cell_lines) 

    if st.button("Create your plot :bar_chart:"):
        with st.spinner("Creating your plot..."):
            if filter_type == "Drugs":
                filtered_df = df[df['DRUG_NAME'] == selected_value] 
                color_by = 'CELL_LINE_NAME'
                title = f"Drug: {selected_value} - Survival Rate vs Drug Concentration"
                legend_title = "Cell Lines"
            else:
                filtered_df = df[df['CELL_LINE_NAME'] == selected_value]
                color_by = 'DRUG_NAME'
                title = f"Cell Line: {selected_value} - Survival Rate vs Drug Concentration"
                legend_title = "Drugs"

            fig = px.scatter(
                filtered_df,
                x=x_axis,
                y=y_axis,
                color=color_by,
                hover_data=['DRUG_NAME', 'CELL_LINE_NAME']
            )
            fig.update_layout(
                title=title,
                xaxis_title=x_axis,
                yaxis_title=y_axis,
                legend_title=legend_title
            )
            st.plotly_chart(fig)
            st.session_state['plot_created'] = True