{% test json_key_not_null(model, column_name, key) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }}:{{ key }}::STRING IS NULL
        OR TRIM({{ column_name }}:{{ key }}::STRING) = ''
{% endtest %}