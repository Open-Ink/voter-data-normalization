WITH VOTER_HISTORY AS (SELECT VOTER_ID,
                              RES_COUNTY_DESC,
                              'PRIMARY'                                              AS ELECTION_TYPE,
                              VOTE_DATE_2022_05_17                                   AS VOTE_DATE,
                              VOTE_TYPE_2022_05_17                                   AS VOTE_TYPE,
                              BALLOT_STYLE_2022_05_17                                AS BALLOT_STYLE,
                              TO_DATE(REPLACE('2022_05_17', '_', '-'), 'YYYY-MM-DD') AS ELECTION_DATE
                       FROM DBT_VOTER_DATA.raw.IDAHO_VOTERS_2023_10_26
                       WHERE VOTE_DATE_2022_05_17 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              RES_COUNTY_DESC,
                              'GENERAL',
                              VOTE_DATE_2022_11_08                                   AS VOTE_DATE,
                              VOTE_TYPE_2022_11_08                                   AS VOTE_TYPE,
                              BALLOT_STYLE_2022_11_08                                AS BALLOT_STYLE,
                              TO_DATE(REPLACE('2022_11_08', '_', '-'), 'YYYY-MM-DD') AS ELECTION_DATE
                       FROM DBT_VOTER_DATA.raw.IDAHO_VOTERS_2023_10_26
                       WHERE VOTE_DATE_2022_11_08 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              RES_COUNTY_DESC,
                              'GENERAL',
                              VOTE_DATE_2018_11_06                                   AS VOTE_DATE,
                              VOTE_TYPE_2018_11_06                                   AS VOTE_TYPE,
                              BALLOT_STYLE_2018_11_06                                AS BALLOT_STYLE,
                              TO_DATE(REPLACE('2018_11_06', '_', '-'), 'YYYY-MM-DD') AS ELECTION_DATE
                       FROM DBT_VOTER_DATA.raw.IDAHO_VOTERS_2023_10_26
                       WHERE VOTE_DATE_2018_11_06 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              RES_COUNTY_DESC,
                              'Presidential primary',
                              VOTE_DATE_2020_03_10                                   AS VOTE_DATE,
                              VOTE_TYPE_2020_03_10                                   AS VOTE_TYPE,
                              BALLOT_STYLE_2020_03_10                                AS BALLOT_STYLE,
                              TO_DATE(REPLACE('2020_03_10', '_', '-'), 'YYYY-MM-DD') AS ELECTION_DATE
                       FROM DBT_VOTER_DATA.raw.IDAHO_VOTERS_2023_10_26
                       WHERE VOTE_DATE_2020_03_10 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              RES_COUNTY_DESC,
                              'Primary',
                              VOTE_DATE_2020_05_19                                   AS VOTE_DATE,
                              VOTE_TYPE_2020_05_19                                   AS VOTE_TYPE,
                              BALLOT_STYLE_2020_05_19                                AS BALLOT_STYLE,
                              TO_DATE(REPLACE('2020_05_19', '_', '-'), 'YYYY-MM-DD') AS ELECTION_DATE
                       FROM DBT_VOTER_DATA.raw.IDAHO_VOTERS_2023_10_26
                       WHERE VOTE_DATE_2020_05_19 IS NOT NULL
                       UNION ALL
                       SELECT VOTER_ID,
                              RES_COUNTY_DESC,
                              'GENERAL',
                              VOTE_DATE_2020_11_03                                   AS VOTE_DATE,
                              VOTE_TYPE_2020_11_03                                   AS VOTE_TYPE,
                              BALLOT_STYLE_2020_11_03                                AS BALLOT_STYLE,
                              TO_DATE(REPLACE('2020_11_03', '_', '-'), 'YYYY-MM-DD') AS ELECTION_DATE
                       FROM DBT_VOTER_DATA.raw.IDAHO_VOTERS_2023_10_26
                       WHERE VOTE_DATE_2020_11_03 IS NOT NULL)
SELECT 'ID'                                                        AS STATE_CODE,
       RES_COUNTY_DESC                                             AS COUNTY_CODE,
       VOTER_ID                                                    AS VOTER_ID,
       ELECTION_TYPE                                               AS ELECTION_TYPE,
       ELECTION_DATE                                               AS ELECTION_DATE,
       CASE
           WHEN ELECTION_TYPE = 'GENERAL'
               THEN NULL
           ELSE SUBSTR(BALLOT_STYLE, LEN(BALLOT_STYLE) - 2, 3) END AS PARTY_VOTED,
       CASE
           WHEN VOTE_TYPE = 'R' THEN 'REGULAR'
           WHEN VOTE_TYPE = 'A' THEN 'ABSENTEE'
           WHEN VOTE_TYPE = 'E' THEN 'EARLY_VOTING'
           ELSE 'UNKNOWN' END                                      AS VOTING_METHOD,
       COALESCE(
               TRY_TO_DATE(VOTE_DATE, 'MM/DD/YYYY'),
               TRY_TO_DATE(VOTE_DATE, 'MM/DD/YYYY HH12:MI:SS AM')
       )                                                           AS DATE_OF_VOTING,
       TRUE                                                        AS WAS_VOTE_COUNTED,
       NULL::object                                                AS ADDITIONAL_DATA
FROM VOTER_HISTORY
WHERE RES_COUNTY_DESC IS NOT NULL