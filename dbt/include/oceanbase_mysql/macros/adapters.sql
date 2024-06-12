{% macro oceanbase_mysql__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create database if not exists {{ relation.without_identifier().include(schema=False) }}
  {%- endcall -%}
{% endmacro %}

{% macro oceanbase_mysql__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier().include(schema=False) }}
  {%- endcall -%}
{% endmacro %}