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

-- Create the stg_cell_lines table
CREATE OR REPLACE TABLE stg_cell_lines(
    cell_line_code VARCHAR(20) PRIMARY KEY UNIQUE,
    cell_line_name VARCHAR(50) NOT NULL UNIQUE,
    user_id VARCHAR(20)
);

-- Create the stg_drugs table
CREATE OR REPLACE TABLE stg_drugs(
    drug_code VARCHAR(20) PRIMARY KEY UNIQUE,
    drug_name VARCHAR(50) NOT NULL UNIQUE,
    user_id VARCHAR(20)
);

-- Create the stg_results table
CREATE OR REPLACE TABLE stg_results(
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
CREATE OR REPLACE PIPE update_stg_cell_lines
    AUTO_INGEST = TRUE
    AS COPY INTO stg_cell_lines
FROM @aws_ext_stage_integration
PATTERN = '.*cell_lines.*\.csv'
FILE_FORMAT = my_csv_format;

CREATE OR REPLACE PIPE update_stg_drugs
    AUTO_INGEST = TRUE
    AS COPY INTO stg_drugs
FROM @aws_ext_stage_integration
PATTERN = '.*drugs.*\.csv'
FILE_FORMAT = my_csv_format;

CREATE OR REPLACE PIPE update_stg_results
    AUTO_INGEST = TRUE
    AS COPY INTO stg_results
FROM @aws_ext_stage_integration
PATTERN = '.*results.*\.csv'
FILE_FORMAT = my_csv_format;

-- Refresh the pipes to ensure data is loaded
ALTER PIPE update_stg_cell_lines REFRESH;
ALTER PIPE update_stg_drugs REFRESH;
ALTER PIPE update_stg_results REFRESH;

-- Create procedure for merging data
CREATE OR REPLACE PROCEDURE merge_into_dim_cell_lines()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO dim_cell_lines AS target
  USING stg_cell_lines AS source
  ON target.cell_line_code = source.cell_line_code
  WHEN MATCHED THEN UPDATE SET target.cell_line_name = source.cell_line_name
  WHEN NOT MATCHED THEN INSERT (cell_line_code, cell_line_name, user_id)
  VALUES (source.cell_line_code, source.cell_line_name, source.user_id);

  RETURN 'Merged dim_cell_lines';
END;
$$;

CREATE OR REPLACE PROCEDURE merge_into_dim_drugs()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO dim_drugs AS target
  USING stg_drugs AS source
  ON target.drug_code = source.drug_code
  WHEN MATCHED THEN UPDATE SET 
    target.drug_name = source.drug_name,
    target.user_id = source.user_id
  WHEN NOT MATCHED THEN 
    INSERT (drug_code, drug_name, user_id)
    VALUES (source.drug_code, source.drug_name, source.user_id);

  RETURN 'Merged dim_drugs';
END;
$$;

CREATE OR REPLACE PROCEDURE merge_into_fac_results()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  MERGE INTO fac_results AS target
  USING stg_results AS source
  ON target.experiment_id = source.experiment_id
  WHEN MATCHED THEN UPDATE SET
    target.experiment_number = source.experiment_number,
    target.cell_line_code = source.cell_line_code,
    target.treatment_time = source.treatment_time,
    target.drug_code = source.drug_code,
    target.drug_concentration = source.drug_concentration,
    target.result_001 = source.result_001,
    target.result_002 = source.result_002,
    target.result_003 = source.result_003,
    target.result_004 = source.result_004,
    target.result_005 = source.result_005,
    target.result_006 = source.result_006,
    target.result_007 = source.result_007,
    target.result_008 = source.result_008,
    target.result_009 = source.result_009,
    target.result_010 = source.result_010,
    target.result_011 = source.result_011,
    target.result_012 = source.result_012,
    target.user_id = source.user_id
  WHEN NOT MATCHED THEN 
    INSERT (
      experiment_id, experiment_number, cell_line_code, treatment_time,
      drug_code, drug_concentration,
      result_001, result_002, result_003, result_004, result_005, result_006,
      result_007, result_008, result_009, result_010, result_011, result_012,
      user_id
    )
    VALUES (
      source.experiment_id, source.experiment_number, source.cell_line_code, source.treatment_time,
      source.drug_code, source.drug_concentration,
      source.result_001, source.result_002, source.result_003, source.result_004, source.result_005, source.result_006,
      source.result_007, source.result_008, source.result_009, source.result_010, source.result_011, source.result_012,
      source.user_id
    );

  RETURN 'Merged fac_results';
END;
$$;

-- Create procedure with truncate tabel and merge
CREATE OR REPLACE PROCEDURE procedure_truncate()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Truncate staging tables first
  TRUNCATE TABLE stg_cell_lines;
  TRUNCATE TABLE stg_drugs;
  TRUNCATE TABLE stg_results;

  -- Run merge statements for each target table
  CALL merge_into_dim_cell_lines();
  CALL merge_into_dim_drugs();
  CALL merge_into_fac_results();

  RETURN 'Truncate and merge completed successfully';
END;
$$;

-- create task
CREATE OR REPLACE TASK task_merge
  WAREHOUSE = 'COMPUTE_WH'
  -- SCHEDULE = '5 MINUTE'
AS
  CALL merge_into_dim_cell_lines();


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