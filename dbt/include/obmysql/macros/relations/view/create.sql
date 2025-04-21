{% macro obmysql__create_view_as(relation, sql) -%}
  {{ get_create_or_replace_view_as(relation, sql, False) }}
{%- endmacro %}

{% macro get_create_or_replace_view_as(relation, sql, replace=False) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set columns = config.get('columns', none) -%}
  {%- set check_option = config.get('check_option', none) -%}

  {{ sql_header if sql_header is not none }}
  create {% if replace %} or replace {% endif %} view {{ relation }}
    {% if columns is not none %}
      (
        {{ ", ".join(columns) }}
      )
    {% endif %}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(sql) }}
    {%- endif %}
  as (
    {{ sql }}
  )
  {% if check_option is not none %}
    {{ check_option }}
  {% endif %}
  ;
{%- endmacro %}