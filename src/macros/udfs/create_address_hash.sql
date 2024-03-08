{% macro create_address_hash() %}

create or replace function {{target.schema}}.CREATE_ADDRESS_HASH(LINE_1 TEXT, LINE_2 TEXT, CITY TEXT, STATE TEXT, ZIPCODE TEXT)
    returns VARCHAR
    language SQL
as
$$
    BASE64_ENCODE(
            UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(LINE_1, LINE_2, CITY, STATE, ZIPCODE), ' '))
    )
$$;

{% endmacro %}