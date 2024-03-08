SELECT 'WI'             AS STATE_CODE,
       REPLACE(
               REPLACE(VH.COUNTY, ' County', ''),
               'St.',
               'Saint') AS COUNTY_CODE,
       VOTER_ID         AS VOTER_ID,
       E.ELECTION_TYPE  AS ELECTION_TYPE,
       E.ELECTION_DATE  AS ELECTION_DATE,
       PARTY            AS PARTY_VOTED,
       CASE
           WHEN VOTE_METHOD IN ('At Polls') THEN 'STANDARD'
           WHEN VOTE_METHOD IN ('Absentee') THEN 'ABSENTEE'
           ELSE 'Unknown'
           END          AS VOTING_METHOD,
       NULL             AS DATE_OF_VOTING,
       TRUE             AS WAS_VOTE_COUNTED,
       NULL::object     AS ADDITIONAL_DATA
FROM {{ source('raw', 'WISCONSIN_VOTER_HISTORY_2023_10_17') }} VH
         INNER JOIN {{ ref("wisconsin_elections") }} E ON VH.ELECTION_DATE = E.ELECTION_MONTH_AND_YEAR

