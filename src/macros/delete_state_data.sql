{% macro delete_state_data(state_code) %}

{% set sql %}

DELETE
FROM {{ ref("all_voters") }}
WHERE STATE_CODE = '{{ state_code }}';

DELETE
FROM {{ ref("all_voter_history") }}
WHERE STATE_CODE = '{{ state_code }}';

{% endset %}

{#{% do log("DELETING STATE " ~ var('state_code'), info=True) %}#}

{% do run_query(sql) %}
{% do log("State Deleted", info=True) %}

{% endmacro %}