import streamlit as st
from data_handler import DataHandler
from snow.snow_handler import SnowflakeHandler

st.set_page_config(
    page_title="Data Analysis"
)

st.title("Data Analysis")

data_handler = DataHandler()
snow_handler = SnowflakeHandler()

if st.session_state['data_updated'] and not st.session_state['data_analyzed']:
    ##TODO: write test for this part
    df = st.session_state['data']
    number = st.number_input("Insert experiment number", value = None, step = 1, min_value = 1)

    if number is not None:
        with st.spinner('Analyzing your data...'):
            experiment_data = data_handler.fetch_experiment_data(data = df, experiment_number = number)
            experiment_data_analyzed, control_means, full_experiment_data = data_handler.analyze_experiment_data(experiment_data)
            user_result = data_handler.calculate_survival(full_experiment_data)

            with st.expander("Analysis Results:"):
                st.write(user_result)

            st.subheader("Select parameters for your plot")
            
            options = list(user_result.columns)
            cell_lines = user_result['CELL_LINE_NAME'].unique()
            drugs = user_result['DRUG_NAME'].unique()
            treatment_times = user_result['TREATMENT_TIME'].unique()

        col1, col2, col3 = st.columns(3)
        with col1:
            x_axis = st.selectbox("Select X-axis:", options, index=options.index('DRUG_CONCENTRATION'))
        with col2:
            y_axis = st.selectbox("Select Y-axis:", options, index=options.index('SURVIVAL_RATE'))
        with col3:
            filter_type = st.radio("Filter by:", ["Drugs", "Cell Lines"], index=0, key="filter_type")
            
        if filter_type == "Drugs":
            selected_value = st.selectbox("Select drug:", drugs)
        else:
            selected_value = st.selectbox("Select cell line:", cell_lines) 
        
        ##TODO: create plots: interactive and publication ready
        if st.button("Create your plot :bar_chart:"):
            with st.spinner("Creating your plot..."):
                figures = data_handler.create_plots(user_result, filter_type, selected_value, x_axis, y_axis, treatment_times)
                for fig in figures:
                    st.plotly_chart(fig)
                st.session_state['plot_created'] = True

        ##TODO: download plots as a report pdf

if snow_handler:
    snow_handler.close_connection()