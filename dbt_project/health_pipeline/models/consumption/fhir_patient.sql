WITH staged AS (
    SELECT * FROM {{ ref('stg_patient') }}
),

fhir AS (
    SELECT
        -- unique patient ID
        patient_id AS id,

        -- full name masked
        '****' AS full_name,

        -- birth date generalised to year only
        birth_year AS birth_date,

        -- standard fields
        gender,
        nationality,
        '****' AS address,

        -- address masked
        insurance_number_hash AS insurance_number,

        -- telecom as JSON with hashed values
        blood_type,

        -- hashed insurance number
        allergies,

        -- additional fields
        last_visit_date,
        created_at,
        lower(trim(marital_status)) AS marital_status,
        json_build_object(
            'phone', phone_hash,
            'email', email_hash
        ) AS telecom

    FROM staged
)

SELECT * FROM fhir
