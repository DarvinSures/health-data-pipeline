/* 
    Test to check if a JSON key is not null in RAW, fundamental test before progressing 
    further into the pipeline
*/
{% test json_key_not_null(model, column_name, key) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }}:{{ key }}::STRING IS NULL
        OR TRIM({{ column_name }}:{{ key }}::STRING) = ''
{% endtest %}