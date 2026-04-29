{% macro create_schemas() %}
    {% set sql %}
        CREATE DATABASE IF NOT EXISTS {{ target.database }};

        CREATE SCHEMA IF NOT EXISTS {{ target.database }}.raw;
        CREATE SCHEMA IF NOT EXISTS {{ target.database }}.uat;
        CREATE SCHEMA IF NOT EXISTS {{ target.database }}.consumption;

        CREATE TABLE IF NOT EXISTS {{ target.database }}.raw.raw_raw_patient (
            loaded_at           TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            source_sheet_id     VARCHAR(255),
            raw_data            VARIANT
        );
    {% endset %}

    {% do run_query(sql) %}
    {% do log("All schemas created successfully in " ~ target.database, info=True) %}
{% endmacro %}