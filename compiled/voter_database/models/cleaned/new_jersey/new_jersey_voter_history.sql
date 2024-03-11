SELECT 'NJ'                 AS STATE_CODE,
       RESIDENCE_COUNTYNAME AS COUNTY_CODE,
       VH.VOTER_ID          AS VOTER_ID,
       ELECTION_TYPE        AS ELECTION_TYPE,
       ELECTION_DATE::date  AS ELECTION_DATE,
       VOTER_PARTY          AS PARTY_VOTED,
       CASE
           WHEN BALLOT_TYPE IN ('Provisional') THEN 'Prov'
           WHEN BALLOT_TYPE IN ('Mail In') THEN 'ABSENTEE'
           ELSE 'STANDARD'
           END              AS VOTING_METHOD,
       NULL                 AS DATE_OF_VOTING,
       TRUE                 AS WAS_VOTE_COUNTED,
       NULL::object         AS ADDITIONAL_DATA
FROM DBT_VOTER_DATA.raw.NEW_JERSEY_VOTER_HISTORY_2023_11_24 VH