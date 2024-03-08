WITH VOTER_HISTORY AS (SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_1 AS TEXT_ELECTION_CODE
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_1 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_2
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_2 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_3
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_3 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_4
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_4 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_5
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_5 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_6
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_6 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_7
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_7 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_8
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_8 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_9
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_9 IS NOT NULL
                       UNION
                       SELECT DB_LOGID, TEXT_REGISTRANT_ID, TEXT_ELECTION_CODE_10
                       FROM {{ source('raw', 'KANSAS_VOTERS_2023_11_06') }}
                       WHERE TEXT_ELECTION_CODE_10 IS NOT NULL)
SELECT 'KS'                                   AS STATE_CODE,
       DB_LOGID                               AS COUNTY_CODE,
       TO_DATE(E.ELECTION_DATE, 'MM/DD/YYYY') AS ELECTION_DATE,
       TEXT_REGISTRANT_ID                     AS VOTER_ID,
       E.ELECTION_TYPE                        AS ELECTION_TYPE,
       NULL                                   AS PARTY_VOTED,
       'UNKNOWN'                              AS VOTING_METHOD,
       NULL                                   AS DATE_OF_VOTING,
       TRUE                                   AS WAS_VOTE_COUNTED,
       NULL::object                           AS ADDITIONAL_DATA
FROM VOTER_HISTORY VH
         INNER JOIN {{ ref("kansas_elections") }} E
                    ON VH.TEXT_ELECTION_CODE = E.ELECTION_CODE