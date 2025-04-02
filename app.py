import streamlit as st
import pandas as pd
import backend
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
    st.session_state["data_uploaded"] = False
    st.session_state["snowflake_connected"] = False
    st.session_state['df'] = None
    st.session_state['parameters_for_plot_selected'] = False
    st.session_state['plot_created'] = False


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

if st.session_state['snowflake_connected']:
    with st.spinner('Loading your data from Snowflake...'):
        backend.wait_for_pipe_completion("update_dim_cell_lines")

# if st.session_state['snowflake_connected'] and not st.session_state['parameters_for_plot_selected']:
#     st.subheader("Select parameters for your plot")
#     df = st.session_state.get('df')
#     if df is not None:
#         options = list(df.columns)
#         cell_lines = df['CELL_LINE_NAME'].str.lower().unique().tolist()
#         drugs = df['DRUG_NAME'].str.lower().unique().tolist()
    
#         col1, col2, col3 = st.columns(3)
#         with col1:
#             x_axis = st.selectbox("Select X-axis:", options, index=options.index('DRUG_CONCENTRATION'))
#         with col2:
#             y_axis = st.selectbox("Select Y-axis:", options, index=options.index('SURVIVAL_RATE_PERCENT'))
#         with col3:
#             filter_type = st.radio("Filter by:", ["Drugs", "Cell Lines"], index=0, key="filter_type")
            
#         if filter_type == "Drugs":
#             selected_value = st.selectbox("Select drug:", drugs)
#         else:
#             selected_value = st.selectbox("Select cell line:", cell_lines) 

#         if st.button("Create your plot :bar_chart:"):
#                 with st.spinner("Creating your plot..."):
#                     if filter_type == "Drugs":
#                         filtered_df = df[df['DRUG_NAME'] == selected_value] 
#                         color_by = 'CELL_LINE_NAME'
#                         title = f"Drug: {selected_value} - Survival Rate vs Drug Concentration"
#                         legend_title = "Cell Lines"
#                     else:
#                         filtered_df = df[df['CELL_LINE_NAME'] == selected_value]
#                         color_by = 'DRUG_NAME'
#                         title = f"Cell Line: {selected_value} - Survival Rate vs Drug Concentration"
#                         legend_title = "Drugs"

#                     fig = px.scatter(
#                         filtered_df,
#                         x=x_axis,
#                         y=y_axis,
#                         color=color_by,
#                         hover_data=['DRUG_NAME', 'CELL_LINE_NAME']
#                     )
#                     fig.update_layout(
#                         title=title,
#                         xaxis_title=x_axis,
#                         yaxis_title=y_axis,
#                         legend_title=legend_title
#                     )
#                     st.plotly_chart(fig)
#                     st.session_state['plot_created'] = True