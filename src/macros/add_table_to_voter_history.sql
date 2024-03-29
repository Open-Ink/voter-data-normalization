{% macro add_table_to_voter_history(table_name) %}

SELECT STATE_CODE,
       COUNTY_CODE,
       VOTER_ID,
       ELECTION_TYPE,
       ELECTION_DATE,
       PARTY_VOTED,
       VOTING_METHOD,
       DATE_OF_VOTING,
       WAS_VOTE_COUNTED,
       ADDITIONAL_DATA,
       CURRENT_TIMESTAMP AS CREATED_AT
FROM {{ table_name }} T1
WHERE NOT EXISTS (SELECT 1
                  FROM {{ this }} T2
                  WHERE T1.STATE_CODE = T2.STATE_CODE
                    AND T1.ELECTION_DATE = T2.ELECTION_DATE)
QUALIFY ROW_NUMBER() OVER (PARTITION BY STATE_CODE, COUNTY_CODE, VOTER_ID, ELECTION_DATE ORDER BY ELECTION_DATE) = 1
{% endmacro %}