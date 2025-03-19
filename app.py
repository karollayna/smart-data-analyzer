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


    save_locally = st.button('Save your result locally')
    save_on_aws = st.button('Save your result on AWS S3 bucket')







### plik zapisywany bedzie w AWS ##################################################
    # directory = r'C:\Users\scigo\Desktop'
    # file_name = 'output.csv'
    # filepath = os.path.join(directory, file_name)
    # user_data.to_csv(filepath, index=True)
    # st.success(f"Plik zapisany pomyślnie w {directory} jako {file_name}")
    # st.line_chart(user_data)
