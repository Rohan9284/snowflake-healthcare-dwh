-- ============================================================
-- 02_load_data.sql
-- Create RAW table, Stage, and Load CSV data
-- ============================================================

USE DATABASE HEALTHCARE_DWH;
USE SCHEMA RAW;
USE WAREHOUSE COMPUTE_WH;

-- Create RAW patients table
CREATE OR REPLACE TABLE PATIENTS (
    patient_id          VARCHAR(50),
    name                VARCHAR(100),
    age                 INT,
    gender              VARCHAR(10),
    blood_type          VARCHAR(5),
    medical_condition   VARCHAR(100),
    date_of_admission   DATE,
    doctor              VARCHAR(100),
    hospital            VARCHAR(100),
    insurance_provider  VARCHAR(100),
    billing_amount      FLOAT,
    room_number         INT,
    admission_type      VARCHAR(50),
    discharge_date      DATE,
    medication          VARCHAR(100),
    test_results        VARCHAR(50),
    length_of_stay      INT
);

-- Create an internal stage to upload the CSV
CREATE OR REPLACE STAGE healthcare_stage
  COMMENT = 'Stage for loading healthcare CSV data';

-- After running this file, upload your CSV using SnowSQL CLI:
-- snowsql -a <account> -u <user>
-- PUT file://data/Cleaned_healthcare_data.csv @healthcare_stage;

-- Load data from stage into table
COPY INTO PATIENTS
FROM @healthcare_stage/Cleaned_healthcare_data.csv
FILE_FORMAT = (
    TYPE                         = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL          = TRUE
)
ON_ERROR = 'CONTINUE';

-- Verify load
SELECT COUNT(*) AS total_records FROM PATIENTS;

-- Quick data quality check
SELECT
    COUNT(*)                                        AS total_rows,
    COUNT(CASE WHEN billing_amount <= 0 THEN 1 END) AS negative_billing,
    COUNT(CASE WHEN patient_id IS NULL THEN 1 END)  AS missing_ids,
    COUNT(CASE WHEN name IS NULL THEN 1 END)        AS missing_names
FROM PATIENTS;
