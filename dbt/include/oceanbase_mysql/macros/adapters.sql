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

{% macro oceanbase_mysql__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    show databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro oceanbase_mysql__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ schema_relation.schema }}' as `schema`,
      '{{ schema_relation.database }}' as `database`,
      table_name as name, 'table' as type
    from information_schema.tables
    where table_schema = '{{ schema_relation.schema }}' and table_type = 'BASE TABLE'
    union all
    select
      '{{ schema_relation.schema }}' as `schema`,
      '{{ schema_relation.database }}' as `database`,
      table_name as name, 'view' as type
    from information_schema.tables
    where table_schema = '{{ schema_relation.schema }}' and table_type like '%VIEW%'
    union all
    select
      '{{ schema_relation.schema }}' as `schema`,
      '{{ schema_relation.database }}' as `database`,
      mview_name as name, 'materialized_view' as type
    from oceanbase.DBA_MVIEWS
    where owner = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro oceanbase_mysql__create_table_as(temporary, relation, sql) -%}
  {%- set external = config.get('external', default=false) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set column_groups = config.get('column_groups', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}
    temporary
  {%- elif external -%}
    external
  {%- endif %} table {{ relation.include(schema=False) }}

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

{% macro oceanbase_mysql__get_empty_schema_sql(columns) %}
    {%- set col_err = [] -%}
    select
    {% for i in columns %}
      {%- set col = columns[i] -%}
      {%- if col['data_type'] is not defined -%}
        {%- do col_err.append(col['name']) -%}
      {%- endif -%}
      {% set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] %}
      {%- if col['data_type'].strip().lower() in ('int', 'bigint') -%}
        cast(null as signed integer) as {{ col_name }}{{ ", " if not loop.last }}
      {% else %}
        cast(null as {{ col['data_type'] }}) as {{ col_name }}{{ ", " if not loop.last }}
      {%- endif -%}
    {%- endfor -%}
    {%- if (col_err | length) > 0 -%}
      {{ exceptions.column_type_missing(column_names=col_err) }}
    {%- endif -%}
{% endmacro %}

{% macro oceanbase_mysql__get_create_index_sql(relation, index_dict) -%}
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

{% macro oceanbase_mysql__copy_grants() %}
    {{ return(False) }}
{% endmacro %}

{% macro oceanbase_mysql__alter_relation_comment(relation, comment) %}
  {%- set external = config.get('external', default=false) -%}
  alter table {%- if external -%}
    external
  {%- endif %} {{ relation }} set comment='{{ comment }}';
{% endmacro %}