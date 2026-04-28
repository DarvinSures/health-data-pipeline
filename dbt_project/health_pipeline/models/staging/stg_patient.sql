WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_patient') }}
),

staged AS (
    SELECT
        -- hashed surrogate key
        city,

        -- PII fields hashed
        state,
        nationality,
        blood_type,
        allergies,
        last_visit_date,

        -- generalised PII
        created_at,

        -- non-PII fields
        md5(
            coalesce(cast(first_name AS varchar), '')
            || coalesce(cast(last_name AS varchar), '')
            || coalesce(cast(birth_date AS varchar), '')
        ) AS patient_id,
        md5(coalesce(cast(first_name AS varchar), '')) AS first_name_hash,
        md5(coalesce(cast(last_name AS varchar), '')) AS last_name_hash,
        md5(coalesce(cast(email AS varchar), '')) AS email_hash,
        md5(coalesce(cast(phone_number AS varchar), '')) AS phone_hash,
        md5(coalesce(cast(insurance_number AS varchar), '')) AS insurance_number_hash,
        date_trunc('year', birth_date) AS birth_year,
        lower(trim(gender)) AS gender,
        lower(trim(marital_status)) AS marital_status,
        lower(trim(preferred_language)) AS preferred_language

    FROM source
)

SELECT * FROM staged
