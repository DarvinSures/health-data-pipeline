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

        -- telecom as JSON with hashed values
        json_build_object(
            'phone', phone_hash,
            'email', email_hash
        )                                               as telecom,

        -- hashed insurance number
        insurance_number_hash                           as insurance_number

    from staged
)

select * from fhir