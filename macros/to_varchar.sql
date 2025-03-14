{% macro to_varchar(column_name) %}
    TRIM(CAST({{ column_name }} AS VARCHAR))
{% endmacro %}