import pandas as pd
import streamlit as st

def update_user_data(uploaded_file, user_column_names):
        
    if uploaded_file is not None:
        user_data = pd.read_csv(uploaded_file, header=0, index_col=0)
        if user_column_names:
            column_names = [str(name) for name in user_column_names.split(',')] #.split(",")
            if len(column_names) == len(user_data.columns):
                user_data.columns = column_names

    return user_data

# uploaded_file = r"C:\Users\scigo\Desktop\portfolio\smart-data-analyzer\simple_data.csv"
# user_column_names = ['skg1','2','3','4','5','6','7','8','9','10','11','12']
# my_obj = update_user_data(uploaded_file, user_column_names)
# # print(my_obj)



def calculate_average_std(my_obj):
    avg = my_obj.mean(axis='rows').round(2)
    st_deviation = my_obj.std(axis='rows').round(2)

    avg_df = avg.to_frame().T
    st_deviation_df = st_deviation.to_frame().T

    avg_df.index = ['avg']
    st_deviation_df.index = ['std']
        
    complete_result = pd.concat([my_obj, avg_df, st_deviation_df])

    chart_df = complete_result.loc[['avg', 'std']].T
    
    return complete_result, avg, st_deviation, chart_df
    

# complete_result, avg, st_deviation, chart_df = calculate_average_std(my_obj)
# print(complete_result)