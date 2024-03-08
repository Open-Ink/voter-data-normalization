SELECT 'FL'                                            AS STATE_CODE,
       C.NAME                                          AS COUNTY_CODE,
       VOTER_ID                                        AS VOTER_ID,
       CASE
           WHEN ELECTION_TYPE = 'GEN' THEN 'GENERAL'
           WHEN ELECTION_TYPE = 'PRI' THEN 'GENERAL PRIMARY'
           WHEN ELECTION_TYPE = 'PPP' THEN 'PPP'
           WHEN ELECTION_TYPE = 'RUN' THEN 'GENERAL RUNOFFS'
           WHEN ELECTION_TYPE = 'OTH' THEN 'OTHER' END AS ELECTION_TYPE,
       ELECTION_DATE::date                             AS ELECTION_DATE,
       NULL                                            AS PARTY_VOTED,
       CASE
           WHEN HISTORY_CODE IN ('A', 'B') THEN 'ABSENTEE'
           WHEN HISTORY_CODE IN ('E') THEN 'EARLY_VOTING'
           WHEN HISTORY_CODE IN ('Y') THEN 'STANDARD'
           ELSE 'STANDARD'
           END                                         AS VOTING_METHOD,
       NULL                                            AS DATE_OF_VOTING,
       CASE
           WHEN HISTORY_CODE IN ('B', 'P')
               THEN FALSE
           ELSE TRUE END                               AS WAS_VOTE_COUNTED,
       NULL::object                                    AS ADDITIONAL_DATA
FROM {{ source('raw', 'FLORIDA_VOTER_HISTORY_2023_08_08') }} V
         LEFT JOIN {{ ref("florida_counties") }} C
                   ON REPLACE(V.COUNTY_CODE, '.', '') = C.CODE