{% macro import_voter_files() %}

{% set sql %}

create schema if not exists RAW;

USE SCHEMA {{target.schema}};

{{ import_georgia_voters() }};

{{ import_kansas_voters() }};

{{ import_michigan_voters() }};

{{ import_arkansas_voters() }};

{{ import_colorado_voters() }};

{{ import_florida_voters() }};

{{ import_iowa_voters() }};

{{ import_maryland_voters() }};

{{ import_nevada_voters() }};

{{ import_new_hampshire_voters() }};

{{ import_pennsylvania_voters() }};

{{ import_normalized_voter_history() }};

{{ import_wisconsin_voters() }};

{{ import_ohio_voters() }};

{{ import_north_carolina_voters() }};

{{ import_south_carolina_voters() }};

{{ import_oregon_voters() }};

{{ import_new_jersey_voters() }};

{{ import_montana_voters() }};

{{ import_texas_voters() }};

{{ import_new_mexico_voters() }};

{{ import_rhode_island_voters() }};

{{ import_idaho_voters() }};

{{ import_mississippi_voters() }};

{{ import_dc_voters() }};

{{ import_washington_voters() }};

{{ import_wyoming_voters() }};

{{ import_louisiana_voters() }};

{{ import_alaska_voters() }};

{{ import_west_virginia_voters() }};

{{ import_alabama_voters() }};

{{ import_delaware_voters() }};

{{ import_nebraska_voters() }};

{{ import_new_york_voters() }};

{{ import_vermont_voters() }};

{{ import_utah_voters() }};

{{ import_missouri_voters() }};

{{ import_oklahoma_voters() }};

{{ import_tennessee_voters() }};

{{ import_virginia_voters() }};

{{ import_connecticut_voters() }};

{{ import_california_voters() }};

{{ import_arizona_voters() }};

{{ import_south_dakota_voters() }}

{% endset %}

{% do run_query(sql) %}
{% do log("Tables imported", info=True) %}

{% endmacro %}