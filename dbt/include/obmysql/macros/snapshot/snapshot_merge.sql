{% macro obmysql__snapshot_merge_sql(target, source, insert_cols) -%}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}

  update
    {{ target }} as dbt_internal_target,
    {{ source }} as dbt_internal_source
  set
    dbt_internal_target.dbt_valid_to = dbt_internal_source.dbt_valid_to
  where
    dbt_internal_source.dbt_scd_id = dbt_internal_target.dbt_scd_id
    and dbt_internal_target.dbt_valid_to is null
    and dbt_internal_source.dbt_change_type in ('update', 'delete');

  insert into {{ target }} ({{ insert_cols_csv }})
   select {% for column in insert_cols -%}
     dbt_internal_source.{{ column }} {%- if not loop.last %}, {%- endif %}
   {%- endfor %}
   from {{ source }} as dbt_internal_source
   where dbt_internal_source.dbt_change_type = 'insert';

{% endmacro %}
