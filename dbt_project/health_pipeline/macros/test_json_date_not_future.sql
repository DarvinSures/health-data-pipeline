{% test json_date_not_future(model, column_name, key) %}
    SELECT *
    FROM {{ model }}
    WHERE TRY_TO_DATE({{ column_name }}:{{ key }}::STRING) > CURRENT_DATE()
{% endtest %}