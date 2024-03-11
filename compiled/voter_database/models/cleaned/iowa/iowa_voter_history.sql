SELECT 'IA'                                       AS STATE_CODE,
       COUNTY                                     AS COUNTY_CODE,
       VOTER_ID                                   AS VOTER_ID,
       ELECTION_DESCRIPTION                       AS ELECTION_TYPE,
       TO_DATE(ELECTION_DATE, 'MM/DD/YYYY')::date AS ELECTION_DATE,
       PARTY                                      AS PARTY_VOTED,
       CASE
           WHEN VOTE_METHOD IN ('A') THEN 'ABSENTEE'
           ELSE 'STANDARD'
           END                                    AS VOTING_METHOD,
       NULL                                       AS DATE_OF_VOTING,
       TRUE                                       AS WAS_VOTE_COUNTED,
       NULL::object                               AS ADDITIONAL_DATA
FROM DBT_VOTER_DATA.raw.IOWA_VOTER_HISTORY_2023_12_14
WHERE VOTE_METHOD != 'DEM'
  AND ELECTION_DATE NOT LIKE '%####################%'