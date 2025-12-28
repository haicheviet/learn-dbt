{% test valid_url(model, column_name) %}
    select *
    from {{ model }}
    where {{ column_name }} not like 'http%'
    -- Simple check for http/https prefix. 
    -- DuckDB regex support can be used for stricter validation:
    -- where not regexp_matches({{ column_name }}, '^https?://')
{% endtest %}
