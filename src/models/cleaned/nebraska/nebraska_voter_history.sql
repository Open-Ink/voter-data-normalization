WITH NEBRASKA_HISTORY
         AS (SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_1 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_1 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_2 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_2 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_3 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_3 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_4 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_4 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_5 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_5 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_6 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_6 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_7 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_7 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_8 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_8 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_9 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_9 IS NOT NULL
             UNION ALL
             SELECT DB_LOGID           AS COUNTY_CODE,
                    TEXT_REGISTRANT_ID AS VOTER_ID,
                    E.ELECTION_TYPE    AS ELECTION_TYPE,
                    E.ELECTION_DATE    AS ELECTION_DATE
             FROM {{ source('raw', 'NEBRASKA_VOTERS_2023_11_01') }} V
                      INNER JOIN {{ ref("nebraska_elections") }} E
                                 ON V.TEXT_ELECTION_CODE_10 = E.ELECTION_CODE
             WHERE V.TEXT_ELECTION_CODE_10 IS NOT NULL)
SELECT 'NE'                                 AS STATE_CODE,
       COUNTY_CODE                          AS COUNTY_CODE,
       TO_DATE(ELECTION_DATE, 'MM/DD/YYYY') AS ELECTION_DATE,
       VOTER_ID                             AS VOTER_ID,
       ELECTION_TYPE                        AS ELECTION_TYPE,
       NULL                                 AS PARTY_VOTED,
       'UNKNOWN'                            AS VOTING_METHOD,
       NULL                                 AS DATE_OF_VOTING,
       TRUE                                 AS WAS_VOTE_COUNTED,
       NULL::object                         AS ADDITIONAL_DATA
FROM NEBRASKA_HISTORY