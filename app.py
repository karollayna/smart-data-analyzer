import streamlit as st
import pandas as pd
import os 

st.title('''
Smart Data Analyzer :female-scientist: :male-scientist:
:dart: This page makes it easy for you to create graphs from your data'
''')

uploaded_file = st.file_uploader('Add your data in file.csv', 
                                 type='csv', 
                                 accept_multiple_files=False, 
                                 disabled=False, 
                                 label_visibility="visible")


if uploaded_file is not None:
    user_data = pd.read_csv(uploaded_file, header=0, index_col=0)
    st.write(user_data)
### plik zapisywany bedzie w AWS ##################################################
    directory = r'C:\Users\scigo\Desktop'
    file_name = 'output.csv'
    filepath = os.path.join(directory, file_name)
    user_data.to_csv(filepath, index=True)
    st.success(f"Plik zapisany pomyślnie w {directory} jako {file_name}")
