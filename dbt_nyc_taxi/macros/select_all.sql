{% macro select_all(source_name, table_name) %}
    {% set cols = adapter.get_columns_in_relation(source(source_name, table_name)) %}
    {%- for col in cols -%}
        {{col.name}}{% if not loop.last %}, {% endif %}
    {%- endfor -%}
{% endmacro %}