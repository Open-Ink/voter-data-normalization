{{
    config(
        materialized='incremental',
        on_schema_change='sync_all_columns'
    )
}}

{{ add_table_to_voter_history(ref("georgia_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("michigan_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("florida_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("iowa_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("maryland_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("nevada_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("new_hampshire_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("pennsylvania_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("new_jersey_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("mississippi_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("alabama_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("arkansas_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("delaware_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("kansas_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("nebraska_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("montana_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("ohio_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("idaho_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("rhode_island_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("washington_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("west_virginia_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("wisconsin_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("vermont_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("utah_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("missouri_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("oklahoma_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("tennessee_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("district_of_columbia_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("connecticut_voter_history")) }}
UNION
{{ add_table_to_voter_history(ref("california_voter_history")) }}