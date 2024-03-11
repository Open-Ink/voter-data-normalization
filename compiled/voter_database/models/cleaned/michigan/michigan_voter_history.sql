SELECT 'MI'                        AS STATE_CODE,
       COUNTY_NAME                 AS COUNTY_CODE,
       VOTER_IDENTIFICATION_NUMBER AS VOTER_ID,
       'Unknown'                   AS ELECTION_TYPE,
       ELECTION_DATE::date         AS ELECTION_DATE,
       NULL                        AS PARTY_VOTED,
       CASE
           WHEN VOTING_TYPE IN ('A') THEN 'ABSENTEE'
           ELSE 'STANDARD'
           END                     AS VOTING_METHOD,
       NULL                        AS DATE_OF_VOTING,
       TRUE                        AS WAS_VOTE_COUNTED,
       NULL::object                AS ADDITIONAL_DATA
FROM DBT_VOTER_DATA.raw.MICHIGAN_VOTER_HISTORY_2023_11_21