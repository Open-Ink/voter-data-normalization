WITH VOTER_HISTORY AS (SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_1 AS ELECTION_CODE,
                              VOTING_METHOD_1  AS VOTING_METHOD
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_1 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_2,
                              VOTING_METHOD_2
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_2 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_3,
                              VOTING_METHOD_3
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_3 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_4,
                              VOTING_METHOD_4
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_4 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_5,
                              VOTING_METHOD_5
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_5 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_6,
                              VOTING_METHOD_6
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_6 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_7,
                              VOTING_METHOD_7
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_7 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_8,
                              VOTING_METHOD_8
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_8 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_9,
                              VOTING_METHOD_9
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_9 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              COUNTY,
                              VOTING_HISTORY_10,
                              VOTING_METHOD_10
                       FROM {{ source('raw', 'DELAWARE_VOTERS_2023_11_17') }}
                       WHERE VOTING_HISTORY_10 IS NOT NULL)
SELECT 'DE'                                   AS STATE_CODE,
       VH.VOTER_ID                            AS VOTER_ID,
       VH.COUNTY                              AS COUNTY_CODE,
       TO_DATE(E.ELECTION_DATE, 'MM/DD/YYYY') AS ELECTION_DATE,
       E.ELECTION_TYPE                        AS ELECTION_TYPE,
       NULL                                   AS PARTY_VOTED,
       CASE
           WHEN VOTING_METHOD = 'P' THEN 'STANDARD'
           WHEN VOTING_METHOD = 'A' THEN 'ABSENTEE'
           WHEN VOTING_METHOD = 'E' THEN 'EARLY_VOTING'
           WHEN VOTING_METHOD = 'V' THEN 'PROVISIONAL'
           END                                AS VOTING_METHOD,
       NULL                                   AS DATE_OF_VOTING,
       TRUE                                   AS WAS_VOTE_COUNTED,
       NULL::object                           AS ADDITIONAL_DATA
FROM VOTER_HISTORY VH
         INNER JOIN {{ ref("delaware_elections") }} E
                    ON VH.ELECTION_CODE = E.ELECTION_CODE