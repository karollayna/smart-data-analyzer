import streamlit as st
from data_handler import DataHandler
from aws.aws_handler import AWSHandler
from snow.snow_handler import SnowflakeHandler
import uuid
import pandas as pd


st.set_page_config(
    page_title="Smart Data Analyzer",
    page_icon=":female-scientist:"
)

st.title(
    """
Smart Data Analyzer :female-scientist: :male-scientist:
:dart: This page makes it easy for you to create graphs from your data
"""
)
st.markdown(
    "Your data should be in the following **format**:",
    help="""
📋 **Expected file format:** `.csv`
📋 **Required files and columns:**

**`data_photodynamic_therapy_cell_lines.csv`**
- `cell_line_code`
- `cell_line_name`

**`data_photodynamic_therapy_drugs.csv`**
- `drug_code`
- `drug_name`
 
**`data_photodynamic_therapy_results.csv`**
- `experiment_id`
- `experiment_number`
- `cell_line_code`
- `treatment_time`
- `drug_code`
- `drug_concentration`
- `result_001 - result_012`

**Note:** The number of `result` columns is fixed at 12.
""",
)