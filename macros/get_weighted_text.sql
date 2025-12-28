{% macro get_weighted_text(weights_dict) %}
    {# 
      Generates a SQL expression to repeat columns based on weights.
      Args:
        weights_dict: A dictionary where keys are column names and values are integer weights.
      Returns:
        SQL string concatenating repeated column values.
    #}
    {% set parts = [] %}
    {% for col, weight in weights_dict.items() %}
        {% do parts.append("repeat(coalesce(" ~ col ~ "::varchar, ''), " ~ weight ~ ")") %}
    {% endfor %}
    
    concat_ws(' ', {{ parts | join(', ') }})
{% endmacro %}
