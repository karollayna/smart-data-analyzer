import pandas as pd
import streamlit as st
import boto3
from datetime import datetime

def upload_user_files():
    # upload user files
    uploaded_files = st.file_uploader(
        'Add your data in file.csv', 
        type='csv', 
        accept_multiple_files=True
        )
    return uploaded_files

def validate_user_data(uploaded_files):
    # Define expected files and columns
    expected_files = {
        'data_photodynamic_therapy_cell_lines.csv': [
            'cell_line_code',
            'cell_line_name'
        ],
        'data_photodynamic_therapy_drugs.csv': [
            'drug_code',
            'drug_name'
        ],
        'data_photodynamic_therapy_results.csv': [
            'experiment_id', 'experiment_number', 'cell_line_code',
            'treatment_time', 'drug_code', 'drug_concentration',
            'result_001', 'result_002','result_003',
            'result_004', 'result_005', 'result_006',
            'result_007','result_008','result_009',
            'result_010', 'result_011', 'result_012'
        ]
    }

    valid_files = []
    unexpected_files = []
    
    for uploaded_file in uploaded_files:
        if uploaded_file.name not in expected_files:
            unexpected_files.append(uploaded_file.name)
            st.error(f''':x: Unexpected files: {unexpected_files}\n
                     Expected files: {list(expected_files.keys())}''')
        else:
            user_data = pd.read_csv(uploaded_file)
            expected_columns = expected_files[uploaded_file.name]
            if user_data.empty:
                st.error(f':x: File "{uploaded_file.name}" is empty.')
                continue
            if list(user_data.columns) != expected_columns:
                st.error(f''':x: File "{uploaded_file.name}" has incorrect columns.\n
                         Expected: {expected_columns}\n Found: {list(user_data.columns)}''')
                continue

            valid_files.append((uploaded_file.name, uploaded_file.getvalue()))
            st.success(f':white_check_mark: File "{uploaded_file.name}" is valid.')
    return valid_files

def upload_files_to_s3(valid_files):
    
    s3_secret_key = st.secrets["AWS_SECRET_ACCESS_KEY"]
    key_id = st.secrets["AWS_ACCESS_KEY_ID"]
    bucket_name = st.secrets["S3_BUCKET_NAME"]
    region_name = st.secrets["AWS_DEFAULT_REGION"]
    s3_user = boto3.client('s3', 
                            aws_access_key_id=key_id, 
                            aws_secret_access_key=s3_secret_key,
                            region_name=region_name)

    for file_name, file_content in valid_files:
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_file_name = f"{current_time}_{file_name}"
        s3_user.put_object(
            Bucket = bucket_name,
            Key = unique_file_name,
            Body = file_content
            )            
        st.success(f':white_check_mark: File "{unique_file_name}" has been uploaded to the cloud.')