import utils
import boto3
from datetime import datetime
import streamlit as st

def upload_files_to_s3(valid_files):

    secrets = utils.load_secrets("secrets.yaml")
    s3_secret_key = secrets["aws_secret_access_key"]
    key_id = secrets["aws_access_key_id"]
    bucket_name = secrets["s3_bucket_name"]
    region_name = secrets["aws_default_region"]
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