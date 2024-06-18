{% macro oceanbase_mysql__drop_view(relation) -%}
    drop view if exists {{ relation.include(schema=False) }}
{%- endmacro %}
