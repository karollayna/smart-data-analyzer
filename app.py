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
    save_on_aws = st.button('Save your result on AWS S3 bucket')
    st.success(f'Your result was saved')

# def expensive_process(option, add):
#     with st.spinner('Processing...'):
#         time.sleep(5)
#     df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6], 'C':[7, 8, 9]}) + add
#     return (df, add)

# cols = st.columns(2)
# option = cols[0].selectbox('Select a number', options=['1', '2', '3'])
# add = cols[1].number_input('Add a number', min_value=0, max_value=10)

# if 'processed' not in st.session_state:
#     st.session_state.processed = {}

# # Process and save results
# if st.button('Process'):
#     result = expensive_process(option, add)
#     st.session_state.processed[option] = result
#     st.write(f'Option {option} processed with add {add}')
#     result[0]





### plik zapisywany bedzie w AWS ##################################################
    # directory = r'C:\Users\scigo\Desktop'
    # file_name = 'output.csv'
    # filepath = os.path.join(directory, file_name)
    # user_data.to_csv(filepath, index=True)
    # st.success(f"Plik zapisany pomyślnie w {directory} jako {file_name}")
    # st.line_chart(user_data)
