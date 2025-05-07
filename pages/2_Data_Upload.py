import streamlit as st
from data_handler import DataHandler
import uuid

st.set_page_config(
    page_title="Data Upload",
    page_icon=":female-scientist:"
)

st.title("Data Upload")

data_handler = DataHandler()

if "data_uploaded" not in st.session_state:
    st.session_state['user_id'] = None
    st.session_state["data_uploaded"] = False
    st.session_state["data_updated"] = False
    st.session_state["data_analyzed"] = False
    st.session_state['data'] = {}
    st.session_state['plot_created'] = False

if st.session_state['user_id'] is None:
    ##TODO: user can choose if they want to generate a new ID or use an existing one
    ##TODO: create a button to generate a new ID
    ##TODO: create a button to use an existing ID
    ##TODO: create a function to check if the ID already exists in the database
    # generate a unique user ID
    full_uuid = uuid.uuid4()
    hex_uuid = full_uuid.hex
    st.session_state['user_id'] = hex_uuid[:10]

with st.container():
    ##TODO: add note to inform the user that the ID is generated automatically or that they can choose an existing one
    st.write(f"**Your Unique ID:** {st.session_state['user_id']}")

if not st.session_state["data_uploaded"]:
    ##TODO: add function to this part
    uploaded_files = data_handler.upload_user_files()
    if uploaded_files:
        valid_files = data_handler.validate_user_data(uploaded_files)
        if valid_files:
            print("Valid files:", valid_files)
            st.session_state["data_uploaded"] = True 

