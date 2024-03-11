WITH VOTER_HISTORY
         AS (SELECT ASCENSION,
                    20 || SUBSTR(VH1, 0, 2) AS ELECTION_YEAR,
                    SUBSTR(VH1, 3, 4)       AS ELECTION_TYPE,
                    SUBSTR(VH1, 8, 1)       AS VOTING_METHOD,
                    VH1
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH1 IS NOT NULL
               AND (SUBSTR(VH1, 3, 4) = 'GENR' OR SUBSTR(VH1, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH2, 0, 2), SUBSTR(VH2, 3, 4), SUBSTR(VH2, 8, 1), VH2
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH2 IS NOT NULL
               AND (SUBSTR(VH2, 3, 4) = 'GENR' OR SUBSTR(VH2, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH3, 0, 2), SUBSTR(VH3, 3, 4), SUBSTR(VH3, 8, 1), VH3
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH3 IS NOT NULL
               AND (SUBSTR(VH3, 3, 4) = 'GENR' OR SUBSTR(VH3, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH4, 0, 2), SUBSTR(VH4, 3, 4), SUBSTR(VH4, 8, 1), VH4
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH4 IS NOT NULL
               AND (SUBSTR(VH4, 3, 4) = 'GENR' OR SUBSTR(VH4, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH5, 0, 2), SUBSTR(VH5, 3, 4), SUBSTR(VH5, 8, 1), VH5
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH5 IS NOT NULL
               AND (SUBSTR(VH5, 3, 4) = 'GENR' OR SUBSTR(VH5, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH6, 0, 2), SUBSTR(VH6, 3, 4), SUBSTR(VH6, 8, 1), VH6
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH6 IS NOT NULL
               AND (SUBSTR(VH6, 3, 4) = 'GENR' OR SUBSTR(VH6, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH7, 0, 2), SUBSTR(VH7, 3, 4), SUBSTR(VH7, 8, 1), VH7
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH7 IS NOT NULL
               AND (SUBSTR(VH7, 3, 4) = 'GENR' OR SUBSTR(VH7, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH8, 0, 2), SUBSTR(VH8, 3, 4), SUBSTR(VH8, 8, 1), VH8
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH8 IS NOT NULL
               AND (SUBSTR(VH8, 3, 4) = 'GENR' OR SUBSTR(VH8, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH9, 0, 2), SUBSTR(VH9, 3, 4), SUBSTR(VH9, 8, 1), VH9
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH9 IS NOT NULL
               AND (SUBSTR(VH9, 3, 4) = 'GENR' OR SUBSTR(VH9, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH10, 0, 2), SUBSTR(VH10, 3, 4), SUBSTR(VH10, 8, 1), VH10
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH10 IS NOT NULL
               AND (SUBSTR(VH10, 3, 4) = 'GENR' OR SUBSTR(VH10, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH11, 0, 2), SUBSTR(VH11, 3, 4), SUBSTR(VH11, 8, 1), VH11
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH11 IS NOT NULL
               AND (SUBSTR(VH11, 3, 4) = 'GENR' OR SUBSTR(VH11, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH12, 0, 2), SUBSTR(VH12, 3, 4), SUBSTR(VH12, 8, 1), VH12
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH12 IS NOT NULL
               AND (SUBSTR(VH12, 3, 4) = 'GENR' OR SUBSTR(VH12, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH13, 0, 2), SUBSTR(VH13, 3, 4), SUBSTR(VH13, 8, 1), VH13
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH13 IS NOT NULL
               AND (SUBSTR(VH13, 3, 4) = 'GENR' OR SUBSTR(VH13, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH14, 0, 2), SUBSTR(VH14, 3, 4), SUBSTR(VH14, 8, 1), VH14
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH14 IS NOT NULL
               AND (SUBSTR(VH14, 3, 4) = 'GENR' OR SUBSTR(VH14, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH15, 0, 2), SUBSTR(VH15, 3, 4), SUBSTR(VH15, 8, 1), VH15
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH15 IS NOT NULL
               AND (SUBSTR(VH15, 3, 4) = 'GENR' OR SUBSTR(VH15, 3, 4) = 'PRIM')
             UNION
             SELECT ASCENSION, 20 || SUBSTR(VH16, 0, 2), SUBSTR(VH16, 3, 4), SUBSTR(VH16, 8, 1), VH16
             FROM DBT_VOTER_DATA.raw.ALASKA_VOTERS_2023_11_13
             WHERE VH16 IS NOT NULL
               AND (SUBSTR(VH16, 3, 4) = 'GENR' OR SUBSTR(VH16, 3, 4) = 'PRIM'))
SELECT 'AK'                                               AS STATE_CODE,
       NULL                                               AS COUNTY_CODE,
       ASCENSION                                          AS VOTER_ID,
       CASE
           WHEN ELECTION_TYPE = 'GENR' THEN 'GENERAL'
           WHEN ELECTION_TYPE = 'PRIM' THEN 'PRIMARY' END AS ELECTION_TYPE,
       CASE
           WHEN ELECTION_TYPE = 'PRIM' THEN NEXT_DAY(NEXT_DAY(NEXT_DAY(ELECTION_YEAR || '-08-01', 'TU'), 'TU'), 'TU')
           WHEN ELECTION_TYPE = 'GENR' THEN NEXT_DAY(NEXT_DAY(ELECTION_YEAR || '-11-01', 'MO'), 'TU')
           END                                            AS ELECTION_DATE,
       NULL                                               AS PARTY_VOTED,
       CASE
           WHEN VOTING_METHOD = 'P' THEN 'STANDARD'
           WHEN VOTING_METHOD = 'E' THEN 'EARLY_VOTING'
           WHEN VOTING_METHOD IN ('A', 'M') THEN 'ABSENTEE'
           WHEN VOTING_METHOD = 'Q' THEN 'PROVISIONAL'
           END                                            AS VOTING_METHOD,
       NULL                                               AS DATE_OF_VOTING,
       TRUE                                               AS WAS_VOTE_COUNTED,
       NULL::object                                       AS ADDITIONAL_DATA
FROM VOTER_HISTORY VH