{% macro create_schemas() %}
    {% set sql %}
        CREATE SCHEMA IF NOT EXISTS landing;
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS staging;
        CREATE SCHEMA IF NOT EXISTS consumption;

        CREATE TABLE IF NOT EXISTS landing.raw_patient (
            _loaded_at      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            _source_sheet_id VARCHAR(255),
            _raw_data        JSONB
        );

        CREATE TABLE IF NOT EXISTS raw.raw_patient (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            birth_date DATE,
            gender VARCHAR(50),
            address VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(100),
            zip_code VARCHAR(20),
            phone_number VARCHAR(50),
            email VARCHAR(100),
            emergency_contact_name VARCHAR(200),
            emergency_contact_phone VARCHAR(50),
            blood_type VARCHAR(20),
            insurance_provider VARCHAR(100),
            insurance_number VARCHAR(100),
            marital_status VARCHAR(50),
            preferred_language VARCHAR(50),
            nationality VARCHAR(100),
            allergies TEXT,
            last_visit_date DATE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    {% endset %}

    {% do run_query(sql) %}
    {% do log("All schemas created successfully", info=True) %}
{% endmacro %}