import streamlit as st

st.set_page_config(
    page_title="Data Processing"
)

st.title("Data Processing")

if st.session_state["data_uploaded"] and not st.session_state['data_updated']:
    ##TODO: find a faster way to do this
    st.write("**Processing your data...**")
    st.write("placeholder")
    st.session_state['data_updated'] = True

   