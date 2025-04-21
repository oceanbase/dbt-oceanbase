{% macro obmysql__get_create_materialized_view_as_sql(relation, sql) %}
    {%- set columns = config.get('columns', none) -%}
    {%- set table_options = config.get('table_options', none) -%}
    {%- set refresh_mode = config.get('refresh_mode', none) -%}
    {%- set check_option = config.get('check_option', none) -%}

    create materialized view {{ relation }}
    {% if columns is not none %}
      (
        {{ ", ".join(columns) }}
      )
    {% endif %}
    {% if table_options is not none %}
        {{ ", ".join(table_options) }}
    {% endif %}
    {% if refresh_mode is not none %}
        {{ refresh_mode }}
    {% endif %}
    as {{ sql }}
    {% if check_option is not none %}
        {{ check_option }}
    {% endif %}
    ;
    {% for _index_dict in config.get('indexes', []) -%}
        {{- get_create_index_sql(relation, _index_dict) -}}
    {%- endfor -%}
{% endmacro %}
