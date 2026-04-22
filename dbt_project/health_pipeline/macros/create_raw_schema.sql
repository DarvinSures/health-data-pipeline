{% macro create_raw_schema() %}
    {% set sql %}
        CREATE SCHEMA IF NOT EXISTS raw;
        
        CREATE TABLE IF NOT EXISTS raw.raw_patient (
            id SERIAL PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            birth_date DATE,
            gender VARCHAR(20),
            address VARCHAR(255),
            city VARCHAR(100),
            state VARCHAR(2),
            zip_code VARCHAR(10),
            phone_number VARCHAR(20),
            email VARCHAR(100),
            emergency_contact_name VARCHAR(200),
            emergency_contact_phone VARCHAR(20),
            blood_type VARCHAR(10),
            insurance_provider VARCHAR(100),
            insurance_number VARCHAR(50),
            marital_status VARCHAR(20),
            preferred_language VARCHAR(50),
            nationality VARCHAR(100),
            allergies TEXT,
            last_visit_date DATE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
    {% endset %}
    
    {% do run_query(sql) %}
    {% do log("Raw schema and table created successfully", info=True) %}
{% endmacro %}