



create schema if not exists PUBLIC;



create or replace function PUBLIC.MERGE_ADDRESS_ELEMENTS(ITEMS ARRAY)
    returns VARCHAR
    language JAVASCRIPT
as
$$
    function mergeStrings(input) {
        if (input == null) {
            return "";
        }
        if (!Array.isArray(input)) {
		    return null;
        }

        let result = "";

        for(var item of input) {
  	        if (item == null || item.trim() == '') {
    	        continue;
            }

            result += item.trim();
            result += " ";
    }

    return result.trim()
    }

    return mergeStrings(ITEMS);
$$;




create or replace function PUBLIC.CREATE_ADDRESS_HASH(LINE_1 TEXT, LINE_2 TEXT, CITY TEXT, STATE TEXT, ZIPCODE TEXT)
    returns VARCHAR
    language SQL
as
$$
    BASE64_ENCODE(
            UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(LINE_1, LINE_2, CITY, STATE, ZIPCODE), ' '))
    )
$$;





