{% macro create_udfs() %}

create schema if not exists {{target.schema}};

{{create_merge_address_elements()}}
{{ create_address_hash() }}

{% endmacro %}