CREATE OR REPLACE DATABASE data_photodynamic_therapy;
USE DATABASE data_photodynamic_therapy;

CREATE OR REPLACE SCHEMA SOURCE_AWS_S3;
USE SCHEMA SOURCE_AWS_S3;

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

CREATE OR REPLACE STAGE SOURCE_AWS_S3
      URL = 's3://smart-data-analyzer-user-results'
      CREDENTIALS = (
        AWS_KEY_ID = 'XXX'
        AWS_SECRET_KEY = 'XXX'
        );

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
NULL_IF = ('NULL', 'null', '', ' ');

COPY INTO dim_cell_lines
FROM @SOURCE_AWS_S3
PATTERN = '.*cell_lines.*\.csv'
FILE_FORMAT = my_csv_format;

COPY INTO dim_drugs
FROM @SOURCE_AWS_S3
PATTERN = '.*drugs.*\.csv'
FILE_FORMAT = my_csv_format;

COPY INTO fac_results
FROM @SOURCE_AWS_S3
PATTERN = '.*results.*\.csv'
FILE_FORMAT = my_csv_format;

SELECT * FROM dim_drugs LIMIT 10;
