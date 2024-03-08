{% macro create_merge_address_elements() %}

create or replace function {{target.schema}}.MERGE_ADDRESS_ELEMENTS(ITEMS ARRAY)
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

{% endmacro %}