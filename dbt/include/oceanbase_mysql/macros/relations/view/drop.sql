{% macro oceanbase_mysql__drop_view(relation) -%}
    drop view id exists {{ relation.include(schema=False) }}
{%- endmacro %}
