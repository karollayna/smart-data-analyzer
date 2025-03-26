import streamlit as st
import pandas as pd
import backend
import plotly.graph_objects as go
from st_files_connection import FilesConnection


st.title('''
Smart Data Analyzer :female-scientist: :male-scientist:
:dart: This page makes it easy for you to create graphs from your data
''')
st.markdown(
    'Your data should be in the following **format**:', 
    help='''
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
'''
)

uploaded_files = backend.upload_user_files()

if uploaded_files:
    valid_files = backend.validate_user_data(uploaded_files)    
    if valid_files:
        if st.button('Save your data :cloud:'):
            with st.spinner('Uploading your data to the cloud...'):
                backend.upload_files_to_s3(valid_files)
                st.success(':white_check_mark: Your data has been saved to the cloud.')