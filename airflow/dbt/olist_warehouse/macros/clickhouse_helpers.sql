{% macro safe_divide(numerator, denominator) %}
    if({{ denominator }} = 0 or {{ denominator }} is null, null, {{ numerator }} / {{ denominator }})
{% endmacro %}
