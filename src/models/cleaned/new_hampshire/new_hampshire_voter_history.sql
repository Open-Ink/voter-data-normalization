SELECT 'NH'                AS STATE_CODE,
       V.COUNTY            AS COUNTY_CODE,
       VH.ID_VOTER         AS VOTER_ID,
       ELECTION_TYPE       AS ELECTION_TYPE,
       ELECTION_DATE::date AS ELECTION_DATE,
       NULL                AS PARTY_VOTED,
       CASE
           WHEN BALLOT_TYPE IN ('Regular') THEN 'STANDARD'
           WHEN BALLOT_TYPE IN ('Absentee') THEN 'ABSENTEE'
           ELSE 'STANDARD'
           END             AS VOTING_METHOD,
       NULL                AS DATE_OF_VOTING,
       TRUE                AS WAS_VOTE_COUNTED,
       NULL::object        AS ADDITIONAL_DATA
FROM {{ source('raw', 'NEW_HAMPSHIRE_VOTER_HISTORY_2023_12_04') }} VH
         LEFT JOIN {{ source('raw', 'NEW_HAMPSHIRE_VOTERS_2023_12_04') }} V ON V.ID_VOTER = VH.ID_VOTER
WHERE V.COUNTY IS NOT NULL
