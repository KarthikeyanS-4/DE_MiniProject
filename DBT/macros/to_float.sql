{% macro to_float(input_value) %}
    CAST(CAST({{ input_value }} AS DECIMAL(10, 2)) AS FLOAT)
{% endmacro %}