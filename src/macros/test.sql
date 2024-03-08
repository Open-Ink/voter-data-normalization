{% macro test() %}

{%- set table_exists = adapter.get_relation(database='DBT_VOTER_DATA',
                                schema='AVI_CLEANED',
                                identifier='ALL_VOTERS1') is not none -%}

{% if not table_exists %}

{{ return(table_exists) }}

{% else %}
{{ print("Table Exists") }}
{{ return(FALSE) }}
{% endif %}
{% endmacro %}