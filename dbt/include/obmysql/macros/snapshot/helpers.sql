{% macro obmysql__post_snapshot(staging_relation) %}
  {# OceanBase MySQL does not support temp table so we should drop the table manually #}
  {{ drop_relation_if_exists(staging_relation) }}
{% endmacro %}