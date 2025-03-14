{% macro to_date(input_value) %}
    TO_DATE({{ input_value }}, 'MM/DD/YYYY')
{% endmacro %}
