# ✨ Smart-Data-Analyzer ✨
designed to make it easy for you to create graphs from your data. 📊

## 🚀 Technologies

![Streamlit](https://img.shields.io/badge/Streamlit-%2300FF00.svg?style=for-the-badge&logo=streamlit&logoColor=white)
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![AWS S3](https://img.shields.io/badge/AWS%20S3-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Plotly](https://img.shields.io/badge/Plotly-%233F4F75.svg?style=for-the-badge&logo=plotly&logoColor=white)
![Git](https://img.shields.io/badge/git-%23F05033.svg?style=for-the-badge&logo=git&logoColor=white)

## 🎥 Demo

![Demo aplikacji](images/smart_data_analyzer_demo.gif)

## 📦 Streamlit App

here will be link 🔗

## 📝 Description

This application is intended for users who want to analyze data from photodynamic therapy experiments. It allows you to easily upload data and then analyze and visualize the results.

## 📁 Requirements

**File Format:** `.csv`
| File Name | Required Columns |
|-----------|------------------|
| `data_photodynamic_therapy_cell_lines.csv` | `cell_line_code`, `cell_line_name` |
| `data_photodynamic_therapy_drugs.csv` | `drug_code`, `drug_name` |
| `data_photodynamic_therapy_results.csv` | `experiment_id`, `experiment_number`, `cell_line_code`, `treatment_time`, `drug_code`, `drug_concentration`, `result_001 - result_012` |

**📥 Sample Files for Completion**

Click below to download the CSV templates you need to fill in:

- [your_cell_lines.csv](sample_files/your_cell_lines.csv)
- [your_drugs.csv](sample_files/your_drugs.csv)
- [your_results.csv](sample_files/your_results.csv)


## 🗂️ Data Architecture
  ![Data Architecture](images/data_architecture.png)

## 📈 Data Flow
  ![Data Flow](images/data_flow.png)

## ⚙️ Project status

The project is currently **in progress 🚘🔜**

## Branch Information:

main branch:
The first working version of the application. It runs in the cloud and is integrated with AWS S3 and Snowflake for data storage and processing.

develop branch:
This is the active development branch where I’m currently working on new features. The application here is designed to run locally.

## ❓ Questions or suggestions

Feel free to reach out with any questions or suggestions! 📧
