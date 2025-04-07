import streamlit as st
import pandas as pd
import plotly.express as px

class DataHandler:
    """
    A class used to handle and process data uploaded by users.

    Attributes:
    ----------
    expected_files : dict
        A dictionary containing expected file names and their respective column names.
    valid_files : list
        A list to store valid files after validation.
    unexpected_files : list
        A list to store unexpected files encountered during validation.
    """

    def __init__(self):
        """
        Initializes the DataHandler class with expected files and empty lists for valid and unexpected files.
        """
        self.expected_files = {
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
                "result_012"
                ]
                }
        self.valid_files = []
        self.unexpected_files = []

    def upload_user_files(self):
        """
        Uploads user files using Streamlit's file uploader.

        Returns:
        -------
        uploaded_files : list
            A list of uploaded files.
        """
        uploaded_files = st.file_uploader(
            "Add your data in file.csv", type="csv", accept_multiple_files=True
        )
        return uploaded_files
    
    def validate_user_data(self, uploaded_files):
        """
        Validates the uploaded files against expected file names and column structures.

        Parameters
        ----------
        uploaded_files : list
            A list of files uploaded by the user via Streamlit's file uploader.

        Returns
        -------
        list of tuple
            A list of valid files, where each item is a tuple (file_name, file_content_bytes).
        """
        for uploaded_file in uploaded_files:
            if uploaded_file.name not in self.expected_files:
                self.unexpected_files.append(uploaded_file.name)
                st.error(
                    f""":x: Unexpected files: {self.unexpected_files}\n
                        Expected files: {list(self.expected_files.keys())}"""
                )
            else:
                user_data = pd.read_csv(uploaded_file)
                expected_columns = self.expected_files[uploaded_file.name]
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
            self.valid_files.append((uploaded_file.name, user_data.to_csv(index=False).encode()))
            st.success(f':white_check_mark: File "{uploaded_file.name}" is valid.')
    
        return self.valid_files

    def fetch_experiment_data(self, data, experiment_number):
        """
        Fetches experiment data for a specific experiment number.

        Parameters:
        ----------
        data : DataFrame
            The DataFrame containing experiment data.
        experiment_number : int
            The number of the experiment to fetch data for.

        Returns:
        -------
        experiment_data : DataFrame
            A DataFrame containing data for the specified experiment number.
        """
        experiment_data = pd.DataFrame(data)
        experiment_data = experiment_data[experiment_data['EXPERIMENT_NUMBER'] == experiment_number]
        
        return experiment_data

    def analyze_experiment_data(self, experiment_data):
        """
        Analyzes the experiment data by calculating mean and standard deviation of result columns.

        Parameters:
        ----------
        experiment_data : DataFrame
            The DataFrame containing experiment data to analyze.

        Returns:
        -------
        experiment_data : DataFrame
            The DataFrame with added 'MEAN' and 'STD' columns.
        control_means : DataFrame
            A DataFrame containing mean values for control groups.
        full_experiment_data : DataFrame
            The DataFrame with merged experiment data and control means.
        """
        results_columns = [col for col in experiment_data.columns if col.startswith('RESULT_')]
        
        experiment_data['MEAN'] = experiment_data[results_columns].mean(axis=1, skipna=True).round(2)
        experiment_data['STD'] = experiment_data[results_columns].std(axis=1, skipna=True).round(2)
        
        controls = experiment_data[(experiment_data['TREATMENT_TIME'] == 0) & (experiment_data['DRUG_CONCENTRATION'] == 0)]
        control_means = controls.groupby(['DRUG_NAME', 'CELL_LINE_NAME'])['MEAN'].mean().reset_index()

        full_experiment_data = experiment_data.merge(control_means, on=['DRUG_NAME', 'CELL_LINE_NAME'], suffixes=('', '_CONTROL'))

        return experiment_data, control_means, full_experiment_data
    
    def calculate_survival(self, full_experiment_data):
        """
        Calculates the survival rate based on mean values and control means.

        Parameters:
        ----------
        full_experiment_data : DataFrame
            The DataFrame containing merged experiment data and control means.

        Returns:
        -------
        full_experiment_data : DataFrame
            The DataFrame with an added 'SURVIVAL_RATE' column.
        """
        
        def calculate_survival_row(row):
            if row['MEAN_CONTROL'] == 0:
                survival = 0  # lub inna wartość, jeśli chcesz uniknąć dzielenia przez zero
            else:
                survival = ((row['MEAN'] / row['MEAN_CONTROL']) * 100)
            
            survival = round(survival, 2)
            return survival

        full_experiment_data['SURVIVAL_RATE'] = full_experiment_data.apply(calculate_survival_row, axis=1)

        return full_experiment_data
    
    def create_plots(self, user_result, filter_type, selected_value, x_axis, y_axis, treatment_times):
        """
        Creates a list of scatter plots based on the filtered data.

        Parameters:
        - **user_result** (DataFrame): The DataFrame containing the user's results.
        - **filter_type** (str): The type of filter to apply. Can be either "Drugs" or "CELL_LINE_NAME".
        - **selected_value** (str): The value to filter by.
        - **x_axis** (str): The column name for the x-axis.
        - **y_axis** (str): The column name for the y-axis.
        - **treatment_times** (list): A list of treatment times to create plots for. Each time should be in a format that can be sorted numerically (e.g., "0 min", "5 min", etc.).

        Returns:
        - **figures** (list): A list of Plotly figures, each representing a scatter plot for a specific treatment time.

        Notes:
        - The function first filters the data based on the specified filter type and value.
        - It then sorts the treatment times in ascending order before creating the plots.
        - If 'CELL_LINE_NAME' or 'DRUG_NAME' columns are present in the filtered data, they are used for coloring the points in the scatter plots.
        """
        def format_axis_title(axis_name):
            return axis_name.replace("_", " ").lower().capitalize()
        
        if filter_type == "Drugs":
            # Filter by drug name
            filtered_df = user_result[user_result['DRUG_NAME'] == selected_value] 
        else:
            # Filter by cell line name
            filtered_df = user_result[user_result['CELL_LINE_NAME'] == selected_value]

        treatment_times = list(treatment_times)
        treatment_times.sort(key=lambda x: int(x.split()[0]) if isinstance(x, str) else x)

        figures = []
        for time in treatment_times:
            # Filter the data for the treatment time
            df_subset = filtered_df[filtered_df['TREATMENT_TIME'] == time]
            
            if 'CELL_LINE_NAME' in df_subset.columns:
                color_col = 'CELL_LINE_NAME'
            elif 'DRUG_NAME' in df_subset.columns:
                color_col = 'DRUG_NAME'
            else:
                color_col = None

            x_axis_title = format_axis_title(x_axis)
            y_axis_title = format_axis_title(y_axis)
            
            fig = px.scatter(
                df_subset,
                x=x_axis,
                y=y_axis,
                color=color_col if color_col else None,
                hover_data=['DRUG_NAME', 'CELL_LINE_NAME'],
                title=f"{x_axis_title} vs {y_axis_title} for {selected_value} at {time} min"
            )
            
            fig.update_layout(
                xaxis_title=x_axis_title,
                yaxis_title=y_axis_title,
                showlegend=True if color_col else False,
                xaxis=dict(showgrid=True),
                yaxis=dict(showgrid=True)
            )
            
            figures.append(fig)
        
        return figures

