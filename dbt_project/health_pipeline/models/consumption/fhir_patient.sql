with staged as (
    select * from {{ ref('stg_patient') }}
),

fhir as (
    select
        -- unique patient ID
        patient_id                                      as id,

        -- full name masked
        '****'                                          as full_name,

        -- birth date generalised to year only
        birth_year                                      as birth_date,

        -- standard fields
        gender,
        nationality,
        lower(trim(marital_status))                     as marital_status,

        -- address masked
        '****'                                          as address,

        -- additional fields
        blood_type,
        allergies,
        last_visit_date,
        created_at

    from staged
)

select * from fhir