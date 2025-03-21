import streamlit as st
import pandas as pd
import backend
import plotly.graph_objects as go


st.title('''
Smart Data Analyzer :female-scientist: :male-scientist:
:dart: This page makes it easy for you to create graphs from your data
''')

uploaded_file = st.file_uploader('Add your data in file.csv', 
                                 type='csv', 
                                 accept_multiple_files=False, 
                                 disabled=False, 
                                 label_visibility="visible")

user_column_names = st.text_input('Add your column names separated by comma')

if st.button('Done!'):
    user_data = backend.update_user_data(uploaded_file, user_column_names)
    complete_result = backend.calculate_average_std(user_data)
    st.write(complete_result)
    graph_avg, graph_std = backend.create_graph(complete_result)

    # https://plotly.com/python/distplot/
    st.line_chart(graph_avg)


if st.button('Save your result locally'):
    uploaded_file_path = r"C:\Users\scigo\Desktop\portfolio\smart-data-analyzer\simple_data.csv"
    user_data = backend.update_user_data(uploaded_file, user_column_names)
    complete_result = backend.calculate_average_std(user_data)
    backend.save_user_result(uploaded_file_path, complete_result)
    # save_on_aws = st.button('Save your result on AWS S3 bucket')
    st.success(f'Your result was saved')

if st.button('Save on AWS s3 bucket'):
    s3_secret_key = st.secrets["AWS_SECRET_ACCESS_KEY"]
    key_id = st.secrets["AWS_ACCESS_KEY_ID"]
    bucket_name = st.secrets["S3_BUCKET_NAME"]
    region_name = st.secrets["AWS_DEFAULT_REGION"]
    aws_connection = backend.AwsHandler(s3_secret_key, key_id, bucket_name, region_name)
    
    with st.spinner("Przesyłanie..."):
        aws_connection.upload(uploaded_file.name)
        st.success(f"Plik {uploaded_file.name} zapisany w S3!")
        st.write('good job!!!')
