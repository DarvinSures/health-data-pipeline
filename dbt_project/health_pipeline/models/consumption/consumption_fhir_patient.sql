WITH uat AS (
    SELECT * FROM {{ ref('uat_patient') }}
),

fhir AS (
    SELECT
        patient_id AS id,
        '****' AS full_name,
        birth_year AS birth_date,
        gender,
        '****' AS address,
        marital_status_code AS marital_status,
        insurance_number_hash AS insurance_number,
        nationality,
        blood_type,
        allergies,
        last_visit_date,
        array_construct(telecom_phone, telecom_email) AS telecom
    FROM uat

    -- if you only wanted rows that pass the test.
    -- WHERE
    --     patient_id IS NOT NULL
    --     AND gender IS NOT NULL
    --     AND gender IN ('male', 'female', 'other', 'unknown')
    --     AND telecom_phone IS NOT NULL
    --     AND telecom_email IS NOT NULL
)

SELECT * FROM fhir
