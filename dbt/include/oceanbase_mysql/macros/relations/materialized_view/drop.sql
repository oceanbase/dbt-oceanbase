{% macro oceanbase_mysql__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation.include(schema=False) }}
{%- endmacro %}
