-- Create a new database
CREATE OR REPLACE DATABASE $DATABASE_NAME;

-- Use the newly created database
USE DATABASE $DATABASE_NAME;

-- Create the dim_cell_lines table
CREATE OR REPLACE TABLE dim_cell_lines(
    cell_line_code VARCHAR(20) PRIMARY KEY UNIQUE,
    cell_line_name VARCHAR(50) NOT NULL UNIQUE,
    user_id VARCHAR(20)
);

-- Create the dim_drugs table
CREATE OR REPLACE TABLE dim_drugs(
    drug_code VARCHAR(20) PRIMARY KEY UNIQUE,
    drug_name VARCHAR(50) NOT NULL UNIQUE,
    user_id VARCHAR(20)
);

-- Create the fac_results table
CREATE OR REPLACE TABLE fac_results(
    experiment_id INT PRIMARY KEY UNIQUE,
    experiment_number INT,
    cell_line_code VARCHAR(20),
    treatment_time INT,
    drug_code VARCHAR(20),
    drug_concentration INT,
    result_001 INT,
    result_002 INT,
    result_003 INT,
    result_004 INT,
    result_005 INT,
    result_006 INT,
    result_007 INT,
    result_008 INT,
    result_009 INT,
    result_010 INT,
    result_011 INT,
    result_012 INT,
    user_id VARCHAR(20)
);

-- Create an external storage integration for S3
CREATE STORAGE INTEGRATION smart_data_analyzer_bucket
    TYPE = EXTERNAL_STAGE
    ENABLED = TRUE
    STORAGE_PROVIDER = 'S3'
    STORAGE_ALLOWED_LOCATIONS = ('s3://$BUCKET_NAME')
    STORAGE_AWS_ROLE_ARN = $AWS_ROLE_ARN;

-- Define a CSV file format
CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '', ' ');

-- Create an external stage using the storage integration
CREATE OR REPLACE STAGE aws_ext_stage_integration
    STORAGE_INTEGRATION = smart_data_analyzer_bucket
    URL = 's3://$BUCKET_NAME'
    FILE_FORMAT = my_csv_format;

-- Create pipes for automatic data loading
CREATE OR REPLACE PIPE update_dim_cell_lines
    AUTO_INGEST = TRUE
    AS COPY INTO dim_cell_lines
FROM @aws_ext_stage_integration
PATTERN = '.*cell_lines.*\.csv'
FILE_FORMAT = my_csv_format;

CREATE OR REPLACE PIPE update_dim_drugs
    AUTO_INGEST = TRUE
    AS COPY INTO dim_drugs
FROM @aws_ext_stage_integration
PATTERN = '.*drugs.*\.csv'
FILE_FORMAT = my_csv_format;

CREATE OR REPLACE PIPE update_fac_results
    AUTO_INGEST = TRUE
    AS COPY INTO fac_results
FROM @aws_ext_stage_integration
PATTERN = '.*results.*\.csv'
FILE_FORMAT = my_csv_format;

-- Refresh the pipes to ensure data is loaded
ALTER PIPE update_dim_cell_lines REFRESH;
ALTER PIPE update_dim_drugs REFRESH;
ALTER PIPE update_fac_results REFRESH;

-- Create a view combining results
CREATE OR REPLACE VIEW combined_results
AS
SELECT
    f.experiment_id,
    f.experiment_number,
    f.user_id,
    c.cell_line_name,
    d.drug_name,
    f.treatment_time,
    f.drug_concentration,
    f.result_001,
    f.result_002,
    f.result_003,
    f.result_004,
    f.result_005,
    f.result_006,
    f.result_007,
    f.result_008,
    f.result_009,
    f.result_010,
    f.result_011,
    f.result_012
FROM
    fac_results f
LEFT JOIN 
    dim_cell_lines c
    ON f.cell_line_code = c.cell_line_code
LEFT JOIN 
    dim_drugs d
    ON f.drug_code = d.drug_code;
