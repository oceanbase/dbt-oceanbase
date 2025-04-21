{% macro obmysql__get_materialized_view_configuration_changes(existing_relation, new_config) %}
    {% set _indexes = config.get('indexes', default=[]) %}
    {% set _config_indexes = [] %}
    {% set _existing_indexes = adapter.list_indexes(existing_relation) %}

    {% for _index_dict in _indexes %}
        {% do _config_indexes.append(adapter.parse_index(_index_dict)) %}
    {% endfor %}

    {% set _configuration_changes = adapter.compare_indexes(_existing_indexes, _config_indexes) %}
    {% do return(_configuration_changes) %}
{% endmacro %}

{% macro obmysql__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}
    {{- log("Applying UPDATE INDEXES to: " ~ relation) -}}

    {%- for _index_change in configuration_changes -%}
        {%- set _index = _index_change.context -%}
        {%- if _index_change.action == "drop" -%}
            {{ get_drop_index_sql(relation, _index.name) }};
        {%- elif _index_change.action == "create" -%}
            {{ get_create_index_sql(relation, _index.to_dict()) }}
        {%- endif -%}
    {%- endfor -%}
{% endmacro %}

