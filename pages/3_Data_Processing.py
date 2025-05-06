import streamlit as st
from data_handler import DataHandler
from aws.aws_handler import AWSHandler
from snow.snow_handler import SnowflakeHandler
import pandas as pd

st.set_page_config(
    page_title="Data Processing"
)

st.title("Data Processing")

data_handler = DataHandler()
aws_handler = AWSHandler()
snow_handler = SnowflakeHandler()

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