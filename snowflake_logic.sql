CREATE OR REPLACE DATABASE database_smart_data_analyzer;

USE DATABASE database_smart_data_analyzer;

CREATE OR REPLACE TABLE dim_cell_lines(
	cell_line_code VARCHAR(20) PRIMARY KEY UNIQUE,
    cell_line_name VARCHAR(50) NOT NULL UNIQUE);
    
CREATE OR REPLACE TABLE dim_drugs(
	drug_code VARCHAR(20) PRIMARY KEY UNIQUE,
    drug_name VARCHAR(50) NOT NULL UNIQUE);

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
    result_012 INT
);

CREATE STORAGE INTEGRATION smart_data_analyzer_bucket
    TYPE = EXTERNAL_STAGE
    ENABLED = TRUE
    STORAGE_PROVIDER = 'S3'
    STORAGE_ALLOWED_LOCATIONS = ('s3://smart-data-analyzer-bucket')
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::390844769419:role/smart-data-analyzer-bucket-role';

-- desc integration smart_data_analyzer_bucket;  
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null', '', ' ');

CREATE OR REPLACE STAGE aws_ext_stage_integration
    STORAGE_INTEGRATION = smart_data_analyzer_bucket
    URL = 's3://smart-data-analyzer-bucket'
    FILE_FORMAT = my_csv_format;

list @aws_ext_stage_integration;

CREATE OR REPLACE PIPE update_dim_cell_lines
    AUTO_INGEST = TRUE
    AS COPY INTO dim_cell_lines
FROM @aws_ext_stage_integration
PATTERN = '.*cell_lines.*\.csv'
FILE_FORMAT = MY_CSV_FORMAT;

CREATE OR REPLACE PIPE update_dim_drugs
    AUTO_INGEST = TRUE
    AS COPY INTO dim_drugs
FROM @aws_ext_stage_integration
PATTERN = '.*drugs.*\.csv'
FILE_FORMAT = MY_CSV_FORMAT;

CREATE OR REPLACE PIPE update_fac_results
    AUTO_INGEST = TRUE
    AS COPY INTO fac_results
FROM @aws_ext_stage_integration
PATTERN = '.*results.*\.csv'
FILE_FORMAT = MY_CSV_FORMAT;
select SYSTEM$PIPE_STATUS('update_fac_results');
alter pipe update_dim_cell_lines refresh;
alter pipe update_dim_drugs refresh;
alter pipe update_fac_results refresh;

select * from dim_cell_lines;
select * from dim_drugs;
select * from fac_results;