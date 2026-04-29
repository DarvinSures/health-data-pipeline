{{ config(materialized='table') }}

/*
    refers to UAT table, defines data types and maps columns to FHIR Patient table
*/

WITH uat AS (
    SELECT * FROM {{ ref('uat_patient') }}
),

fhir AS (
    SELECT
        -- PRIMARY KEY
        patient_id::VARCHAR(255) AS id,

        -- Mapping hash to the full_name column
        full_name_hash::VARCHAR(200) AS full_name,

        -- Ensuring birth_year (DATE type from date_trunc) is mapped to birth_date
        birth_date::DATE AS birth_date,

        gender::VARCHAR(20) AS gender,

        -- Mapping address_hash to address
        address_hash::VARCHAR(255) AS address,

        -- CHANGED: Using object_construct instead of array_construct 
        -- to create a JSON object with 'phone' and 'email' fields.
        marital_status_code::VARCHAR(20) AS marital_status,

        insurance_number_hash::VARCHAR(255) AS insurance_number,

        nationality::VARCHAR(20) AS nationality,

        object_construct(
            'phone', telecom_phone,
            'email', telecom_email
        ) AS telecom
    FROM uat
)

SELECT * FROM fhir
