{% macro oceanbase_mysql__drop_table(relation) -%}
    drop table if exists {{ relation.include(schema=False) }}
{%- endmacro %}
