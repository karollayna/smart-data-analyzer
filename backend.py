import pandas as pd
import streamlit as st
import boto3
from datetime import datetime
import snowflake.connector
import time

def upload_user_files():
    # upload user files
    uploaded_files = st.file_uploader(
        "Add your data in file.csv", type="csv", accept_multiple_files=True
    )
    return uploaded_files


def validate_user_data(uploaded_files):
    # Define expected files and columns
    expected_files = {
        "data_photodynamic_therapy_cell_lines.csv": [
            "cell_line_code",
            "cell_line_name",
        ],
        "data_photodynamic_therapy_drugs.csv": ["drug_code", "drug_name"],
        "data_photodynamic_therapy_results.csv": [
            "experiment_id",
            "experiment_number",
            "cell_line_code",
            "treatment_time",
            "drug_code",
            "drug_concentration",
            "result_001",
            "result_002",
            "result_003",
            "result_004",
            "result_005",
            "result_006",
            "result_007",
            "result_008",
            "result_009",
            "result_010",
            "result_011",
            "result_012",
        ],
    }

    valid_files = []
    unexpected_files = []

    for uploaded_file in uploaded_files:
        if uploaded_file.name not in expected_files:
            unexpected_files.append(uploaded_file.name)
            st.error(
                f""":x: Unexpected files: {unexpected_files}\n
                     Expected files: {list(expected_files.keys())}"""
            )
        else:
            user_data = pd.read_csv(uploaded_file)
            expected_columns = expected_files[uploaded_file.name]
            if user_data.empty:
                st.error(f':x: File "{uploaded_file.name}" is empty.')
                continue
            if list(user_data.columns) != expected_columns:
                st.error(
                    f""":x: File "{uploaded_file.name}" has incorrect columns.\n
                         Expected: {expected_columns}\n Found: {list(user_data.columns)}"""
                )
                continue

        user_data['user_id'] = st.session_state['user_id']
        
        temp_file = f"temp_{uploaded_file.name}"
        user_data.to_csv(temp_file, index=False)

        valid_files.append((temp_file, open(temp_file, 'rb').read()))
        st.success(f':white_check_mark: File "{uploaded_file.name}" is valid.')
    
    return valid_files


def upload_files_to_s3(valid_files):

    s3_secret_key = st.secrets["AWS_SECRET_ACCESS_KEY"]
    key_id = st.secrets["AWS_ACCESS_KEY_ID"]
    bucket_name = st.secrets["S3_BUCKET_NAME"]
    region_name = st.secrets["AWS_DEFAULT_REGION"]
    s3_user = boto3.client(
        "s3",
        aws_access_key_id=key_id,
        aws_secret_access_key=s3_secret_key,
        region_name=region_name,
    )

    for file_name, file_content in valid_files:
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_file_name = f"{current_time}_{file_name}"
        s3_user.put_object(Bucket=bucket_name, Key=unique_file_name, Body=file_content)
        st.success(
            f':white_check_mark: File "{unique_file_name}" has been uploaded to the cloud.'
        )
    return unique_file_name


def connect_with_snowflake():

    snowflake_account = st.secrets["SNOWFLAKE_ACCOUNT"]
    snowflake_user = st.secrets["SNOWFLAKE_USER"]
    snowflake_password = st.secrets["SNOWFLAKE_PASSWORD"]
    snowflake_warehouse = st.secrets["SNOWFLAKE_WAREHOUSE"]
    snowflake_database = st.secrets["SNOWFLAKE_DATABASE"]
    snowflake_schema = st.secrets["SNOWFLAKE_SCHEMA"]

    conn = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema,
    )

    return conn

def refresh_snowpipe(pipe_name):
    conn = connect_with_snowflake()
    cur = conn.cursor()
    cur.execute(f"ALTER PIPE {pipe_name} REFRESH;")
    time.sleep(10)
    conn.close()
    return "PIPE refreshed!"

def fetch_data(table_name):
    conn = connect_with_snowflake()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table_name};")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return columns, pd.DataFrame(rows, columns=columns)

def fetch_full_data(view_name, user_id):
    conn = connect_with_snowflake()
    cur = conn.cursor()
    query = f"SELECT DISTINCT * FROM {view_name} WHERE USER_ID = %s"
    cur.execute(query, (user_id,))
    result = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    df = pd.DataFrame(result, columns=columns)
    
    return df


def fetch_experiment_data(data, experiment_number):

    df = pd.DataFrame(data)
    experiment_data = df[df['EXPERIMENT_NUMBER'] == experiment_number]
    
    return experiment_data


def analyze_experiment_data(experiment_data):
  
    results_columns = [col for col in experiment_data.columns if col.startswith('RESULT_')]
    
    experiment_data['MEAN'] = experiment_data[results_columns].mean(axis=1, skipna=True).round(2)
    experiment_data['STD'] = experiment_data[results_columns].std(axis=1, skipna=True).round(2)
    
    controls = experiment_data[(experiment_data['TREATMENT_TIME'] == 0) & (experiment_data['DRUG_CONCENTRATION'] == 0)]
    control_means = controls.groupby(['DRUG_NAME', 'CELL_LINE_NAME'])['MEAN'].mean().reset_index()

    return experiment_data, control_means


def calculate_survival(experiment_data, control_means):
    def calculate_survival_row(row):
        control_key = (row['DRUG_NAME'], row['CELL_LINE_NAME'])
        control_mean = control_means[(control_means['DRUG_NAME'] == control_key[0]) & (control_means['CELL_LINE_NAME'] == control_key[1])]['MEAN'].values[0]
        
        if control_mean == 0:
            control_mean = 1 
        
        survival = ((row['MEAN'] / control_mean) * 100)
        survival = round(survival, 2)

        return survival
    
    experiment_data['SURVIVAL_RATE'] = experiment_data.apply(calculate_survival_row, axis=1)
    
    return experiment_data