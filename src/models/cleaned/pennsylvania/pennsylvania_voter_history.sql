SELECT 'PA'                 AS STATE_CODE,
       V.COUNTY             AS COUNTY_CODE,
       VH.VOTER_ID          AS VOTER_ID,
       ELECTION_DESCRIPTION AS ELECTION_TYPE,
       ELECTION_DATE::date  AS ELECTION_DATE,
       NULL                 AS PARTY_VOTED,
       CASE
           WHEN VOTE_METHOD IN ('AP') THEN 'STANDARD'
           WHEN VOTE_METHOD IN ('MB', 'AB') THEN 'ABSENTEE'
           WHEN VOTE_METHOD IN ('P') THEN 'PROVISIONAL'
           ELSE 'STANDARD'
           END              AS VOTING_METHOD,
       NULL                 AS DATE_OF_VOTING,
       TRUE                 AS WAS_VOTE_COUNTED,
       NULL::object         AS ADDITIONAL_DATA
FROM {{ source('raw', 'PENNSLYVANIA_VOTER_HISTORY_2023_11_13') }} VH
         LEFT JOIN {{ source('raw', 'PENNSYLVANIA_VOTERS_2023_11_13') }} V ON V.ID_NUMBER = VH.VOTER_ID
WHERE V.COUNTY IS NOT NULL