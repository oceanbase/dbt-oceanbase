{% macro obmysql__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create database if not exists {{ relation.without_identifier().include(schema=False) }}
  {%- endcall -%}
{% endmacro %}

{% macro obmysql__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier().include(schema=False) }}
  {%- endcall -%}
{% endmacro %}

{% macro obmysql__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro obmysql__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.database }}' as `database`,
      table_name as name,
      '{{ schema_relation.schema }}' as `schema`,
      'table' as type
    from information_schema.tables
    where table_schema = '{{ schema_relation.schema }}' and table_type = 'BASE TABLE'
    union all
    select
      '{{ schema_relation.database }}' as `database`,
      table_name as name,
      '{{ schema_relation.schema }}' as `schema`,
      'view' as type
    from information_schema.tables
    where table_schema = '{{ schema_relation.schema }}' and table_type like '%VIEW%'
    union all
    select
      '{{ schema_relation.database }}' as `database`,
      mview_name as name,
      '{{ schema_relation.schema }}' as `schema`,
      'materialized_view' as type
    from oceanbase.DBA_MVIEWS
    where owner = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro obmysql__get_empty_schema_sql(columns) %}
    {%- set col_err = [] -%}
    select
    {% for i in columns %}
      {%- set col = columns[i] -%}
      {%- if col['data_type'] is not defined -%}
        {%- do col_err.append(col['name']) -%}
      {%- endif -%}
      {% set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] %}
      cast(null as {{ adapter.translate_cast_type(col['data_type']) }}) as {{ col_name }}{{ ", " if not loop.last }}
    {%- endfor -%}
    {%- if (col_err | length) > 0 -%}
      {{ exceptions.column_type_missing(column_names=col_err) }}
    {%- endif -%}
{% endmacro %}

{% macro obmysql__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set comma_separated_columns = ", ".join(index_config.columns) -%}
  {%- set index_name = index_config.get_name(relation) -%}

  create {% if index_config.unique -%}
    unique
  {%- endif %} index if not exists `{{ index_name }}`
  {% if index_config.algorithm -%}
    using {{ index_config.algorithm }}
  {%- endif %}
  on {{ relation }}
  ({{ comma_separated_columns }})
  {% if index_config.options is not none %}
    {{ " ".join(index_config.options) }}
  {% endif %}
  {% if index_config.column_groups is not none %}
    with column group (
    {{ ", ".join(index_config.column_groups) }}
    )
  {% endif %};
{%- endmacro %}

{% macro obmysql__copy_grants() %}
    {{ return(False) }}
{% endmacro %}

{% macro obmysql__alter_relation_comment(relation, comment) %}
  {%- if not relation.is_table -%}
    {{ exceptions.raise_fail_fast_error(relation.type ~ " do not support setting comment") }}
  {%- endif -%}
  alter table {{ relation }} set comment='{{ comment }}';
{% endmacro %}

{% macro obmysql__rename_relation(from_relation, to_relation) -%}
  {#
    2-step process is needed:
    1. Drop the existing relation
    2. Rename the new relation to existing relation
  #}
  {% call statement('drop_relation') %}
    drop {{ to_relation.type }} if exists {{ to_relation }} cascade
  {% endcall %}
  {% call statement('rename_relation') %}
    rename table {{ from_relation }} to {{ to_relation }}
  {% endcall %}
{% endmacro %}

{% macro obmysql__get_show_indexes_sql(relation) %}
    show index from `{{ relation.identifier }}` from `{{ relation.database }}`
{% endmacro %}

{% macro obmysql__list_indexes(relation) %}
    {% call statement('list_indexes', fetch_result=True, auto_begin=False) %}
        {{ get_show_indexes_sql(relation) }}
    {% endcall %}
    {{ return(load_result('list_indexes').table) }}
{% endmacro %}

{%- macro obmysql__get_drop_index_sql(relation, index_name) -%}
    drop index `{{ index_name }}` on {{ relation }}
{%- endmacro -%}

{% macro obmysql__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale
      from {{ relation.information_schema('columns') }}
      where table_name = '{{ relation.identifier }}'
      {% if relation.database %}
        and table_schema = '{{ relation.database }}'
      {% endif %}
      order by ordinal_position
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}