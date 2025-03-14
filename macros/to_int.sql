{% macro to_int(input_value) %}
    COALESCE(CAST({{ input_value }} AS INT),0)
{% endmacro %}
