with source as (
    select * from {{ source('raw', 'raw_patient') }}
),

staged as (
    select
        -- hashed surrogate key
        md5(
            coalesce(cast(first_name as varchar), '') ||
            coalesce(cast(last_name as varchar), '') ||
            coalesce(cast(birth_date as varchar), '')
        )                                               as patient_id,

        -- PII fields hashed
        md5(coalesce(cast(first_name as varchar), '')) as first_name_hash,
        md5(coalesce(cast(last_name as varchar), ''))  as last_name_hash,
        md5(coalesce(cast(email as varchar), ''))       as email_hash,
        md5(coalesce(cast(phone_number as varchar), '')) as phone_hash,
        md5(coalesce(cast(insurance_number as varchar), '')) as insurance_number_hash,

        -- generalised PII
        date_trunc('year', birth_date)                  as birth_year,

        -- non-PII fields
        lower(trim(gender))                             as gender,
        city,
        state,
        lower(trim(marital_status))                     as marital_status,
        lower(trim(preferred_language))                 as preferred_language,
        nationality,
        blood_type,
        allergies,
        last_visit_date,
        created_at

    from source
)

select * from staged