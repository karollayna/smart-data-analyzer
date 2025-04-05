import boto3
from datetime import datetime
import streamlit as st
from utils import load_secrets

class AWSHandler:
    def __init__(self):
        secrets = load_secrets("secrets.yaml")
        self.s3_secret_key = secrets["aws_secret_access_key"]
        self.key_id = secrets["aws_access_key_id"]
        self.bucket_name = secrets["s3_bucket_name"]
        self.region_name = secrets["aws_default_region"]
        self.s3_user = boto3.client(
            "s3",
            aws_access_key_id=self.key_id,
            aws_secret_access_key=self.s3_secret_key,
            region_name=self.region_name,
        )

    def upload_files_to_s3(self, valid_files):
        """
        Uploads files to an S3 bucket.

        Parameters:
        ----------
        valid_files : list
            A list of tuples containing file names and their contents.

        Returns:
        -------
        uploaded_file_names : list
            A list of unique file names uploaded to S3.
        """
        uploaded_file_names = []
        for file_name, file_content in valid_files:
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_file_name = f"{current_time}_{file_name}"
            self.s3_user.put_object(Bucket=self.bucket_name, Key=unique_file_name, Body=file_content)
            st.success(
                f':white_check_mark: File "{unique_file_name}" has been uploaded to the cloud.'
            )
            uploaded_file_names.append(unique_file_name)
        return uploaded_file_names