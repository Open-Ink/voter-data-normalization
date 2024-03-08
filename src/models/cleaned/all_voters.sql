{{
    config(
        materialized='incremental',
        on_schema_change='sync_all_columns'
    )
}}

{{ add_table_to_all_voters(ref('georgia_voters')) }}
UNION
{{ add_table_to_all_voters(ref('kansas_voters')) }}
UNION
{{ add_table_to_all_voters(ref('michigan_voters')) }}
UNION
{{ add_table_to_all_voters(ref('arkansas_voters')) }}
UNION
{{ add_table_to_all_voters(ref('colorado_voters')) }}
UNION
{{ add_table_to_all_voters(ref('florida_voters')) }}
UNION
{{ add_table_to_all_voters(ref('iowa_voters')) }}
UNION
{{ add_table_to_all_voters(ref('maryland_voters')) }}
UNION
{{ add_table_to_all_voters(ref('nevada_voters')) }}
UNION
{{ add_table_to_all_voters(ref('new_hampshire_voters')) }}
UNION
{{ add_table_to_all_voters(ref('pennsylvania_voters')) }}
UNION
{{ add_table_to_all_voters(ref('wisconsin_voters')) }}
UNION
{{ add_table_to_all_voters(ref('ohio_voters')) }}
UNION
{{ add_table_to_all_voters(ref('north_carolina_voters')) }}
UNION
{{ add_table_to_all_voters(ref('south_carolina_voters')) }}
UNION
{{ add_table_to_all_voters(ref('oregon_voters')) }}
UNION
{{ add_table_to_all_voters(ref('montana_voters')) }}
UNION
{{ add_table_to_all_voters(ref('texas_voters')) }}
UNION
{{ add_table_to_all_voters(ref('new_jersey_voters')) }}
UNION
{{ add_table_to_all_voters(ref('idaho_voters')) }}
UNION
{{ add_table_to_all_voters(ref('mississippi_voters')) }}
UNION
{{ add_table_to_all_voters(ref('new_mexico_voters')) }}
UNION
{{ add_table_to_all_voters(ref('washington_voters')) }}
UNION
{{ add_table_to_all_voters(ref('louisiana_voters')) }}
UNION
{{ add_table_to_all_voters(ref('wyoming_voters')) }}
UNION
{{ add_table_to_all_voters(ref('west_virginia_voters')) }}
UNION
{{ add_table_to_all_voters(ref('alabama_voters')) }}
UNION
{{ add_table_to_all_voters(ref('colorado_voters')) }}
UNION
{{ add_table_to_all_voters(ref('delaware_voters')) }}
UNION
{{ add_table_to_all_voters(ref('nebraska_voters')) }}
UNION
{{ add_table_to_all_voters(ref('rhode_island_voters')) }}
UNION
{{ add_table_to_all_voters(ref('vermont_voters')) }}
UNION
{{ add_table_to_all_voters(ref('utah_voters')) }}
UNION
{{ add_table_to_all_voters(ref('missouri_voters')) }}
UNION
{{ add_table_to_all_voters(ref('oklahoma_voters')) }}
UNION
{{ add_table_to_all_voters(ref('tennessee_voters')) }}
UNION
{{ add_table_to_all_voters(ref('virginia_voters')) }}
UNION
{{ add_table_to_all_voters(ref('district_of_columbia_voters')) }}
UNION
{{ add_table_to_all_voters(ref('connecticut_voters')) }}
UNION
{{ add_table_to_all_voters(ref('new_york_voters')) }}
UNION
{{ add_table_to_all_voters(ref('california_voters')) }}
UNION
{{ add_table_to_all_voters(ref('arizona_voters')) }}
UNION
{{ add_table_to_all_voters(ref('south_dakota_voters')) }}