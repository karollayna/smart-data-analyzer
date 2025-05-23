import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="Data Processing"
)

st.title("Data Processing")

if st.session_state['user_id'] and st.session_state["data_uploaded"]:
    st.write("**Processing your data...**")
        
        

if st.session_state["data_uploaded"]:

    partial_file_names = ['cell_lines.csv', 'drugs.csv', 'results.csv']
    dfs = {}

    for partial_file_name in partial_file_names:
        for file_name, df in st.session_state['data'].items():
            if partial_file_name in file_name:
                dfs[partial_file_name] = df

    st.session_state['data_updated'] = True

    for name in partial_file_names:
        if name in dfs:
            merged_df = dfs['results.csv'].merge(dfs['cell_lines.csv'][['cell_line_code', 'cell_line_name']],
                                                 on = 'cell_line_code',
                                                 how = 'left').merge(dfs['drugs.csv'][['drug_code', 'drug_name']], on = 'drug_code', how = 'left')
    final_df = merged_df.drop(columns=['cell_line_code', 'drug_code'])
    st.write("### Zmergowany DataFrame:")
    st.dataframe(final_df)

   