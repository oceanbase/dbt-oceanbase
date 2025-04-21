{% macro obmysql__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set column_groups = config.get('column_groups', none) -%}

  {{ sql_header if sql_header is not none }}

  create table {{ relation.include(schema=False) }}

  {% if column_groups is not none %}
    with column group (
    {%- for column_group in column_groups -%}
        {{- column_group -}}
        {{ ", " if not loop.last }}
    {%- endfor -%}
    )
  {% endif %}

  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {% endif -%}
  {% if contract_config.enforced and (not temporary) -%}
      {{ get_table_columns_and_constraints() }} ;
    insert into {{ relation.include(schema=False) }} (
      {{ adapter.dispatch('get_column_names', 'dbt')() }}
    )
    {%- set sql = get_select_subquery(sql) %}
  {% else %}
    as
  {% endif %}
  (
    {{ sql }}
  );
{%- endmacro %}