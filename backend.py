import pandas as pd
import streamlit as st
import os

def update_user_data(uploaded_file_path, user_column_names):
        
    if uploaded_file_path is not None:
        user_data = pd.read_csv(uploaded_file_path, header=0, index_col=0)
        if user_column_names:
            column_names = [str(name) for name in user_column_names.split(",")] #.split(",")
            if len(column_names) == len(user_data.columns):
                user_data.columns = column_names

    return user_data


def calculate_average_std(my_obj):
    avg = my_obj.mean(axis='rows').round(2)
    st_deviation = my_obj.std(axis='rows').round(2)

    avg_df = avg.to_frame().T
    st_deviation_df = st_deviation.to_frame().T

    avg_df.index = ['avg']
    st_deviation_df.index = ['std']
        
    complete_result = pd.concat([my_obj, avg_df, st_deviation_df])
    
    return complete_result


def create_graph(complete_result):
    chart_df = complete_result.loc[['avg', 'std']].T
    avg_data = chart_df['avg']
    std_data = chart_df['std']
    return avg_data, std_data

def save_user_result(uploaded_file_path, complete_result):
    current_directory = os.path.dirname(uploaded_file_path)
    destination_folder_name = "data_analysis"
    destination_folder_path = os.path.join(current_directory, destination_folder_name)
    if not os.path.exists(destination_folder_path):
        os.makedirs(destination_folder_path)
        print(f"Folder '{destination_folder_name}' został utworzony w katalogu: {current_directory}")
    else:
        print(f"Folder '{destination_folder_name}' już istnieje w katalogu: {current_directory}")
    
    csv_file_name = os.path.splitext(os.path.basename(uploaded_file_path))[0] + '.csv'
    csv_file_path = os.path.join(destination_folder_path, csv_file_name)

    complete_result.to_csv(csv_file_path, index=False)
    
    print(f"Wynik zapisano do pliku: {csv_file_path}")


# uploaded_file_path = r"C:\Users\scigo\Desktop\portfolio\smart-data-analyzer\simple_data.csv"
# user_column_names = ['skg1','2','3','4','5','6','7','8','9','10','11','12']
# my_obj = update_user_data(uploaded_file_path, user_column_names)
# # print(my_obj)
    

# complete_result = calculate_average_std(my_obj)
# print(complete_result)


# save_user_result(uploaded_file_path, complete_result)
# print(pd.read_csv(r"C:\Users\scigo\Desktop\portfolio\smart-data-analyzer\data_analysis\simple_data.csv"))

