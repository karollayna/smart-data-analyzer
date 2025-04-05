import streamlit as st
import pandas as pd

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

        Parameters:
        ----------
        uploaded_files : list
            A list of files uploaded by the user.

        Returns:
        -------
        valid_files : list
            A list of valid files after validation.
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
            
            temp_file = f"temp_{uploaded_file.name}"
            user_data.to_csv(temp_file, index=False)

            self.valid_files.append((temp_file, open(temp_file, 'rb').read()))
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
        """
        results_columns = [col for col in experiment_data.columns if col.startswith('RESULT_')]
        
        experiment_data['MEAN'] = experiment_data[results_columns].mean(axis=1, skipna=True).round(2)
        experiment_data['STD'] = experiment_data[results_columns].std(axis=1, skipna=True).round(2)
        
        controls = experiment_data[(experiment_data['TREATMENT_TIME'] == 0) & (experiment_data['DRUG_CONCENTRATION'] == 0)]
        control_means = controls.groupby(['DRUG_NAME', 'CELL_LINE_NAME'])['MEAN'].mean().reset_index()

        return experiment_data, control_means

    def calculate_survival(self, experiment_data, control_means):
        """
        Calculates the survival rate based on mean values and control means.

        Parameters:
        ----------
        experiment_data : DataFrame
            The DataFrame containing experiment data.
        control_means : DataFrame
            A DataFrame containing mean values for control groups.

        Returns:
        -------
        experiment_data : DataFrame
            The DataFrame with an added 'SURVIVAL_RATE' column.
        """
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