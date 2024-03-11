WITH MO_HISTORY
         AS (SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_1, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_1, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_1 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_2, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_2, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_2 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_3, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_3, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_3 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_4, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_4, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_4 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_5, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_5, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_5 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_6, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_6, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_6 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_7, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_7, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_7 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_8, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_8, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_8 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_9, 0, 10))                       AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_9, 12)))                   AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_9 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_10, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_10, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_10 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_11, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_11, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_11 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_12, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_12, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_12 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_13, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_13, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_13 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_14, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_14, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_14 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_15, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_15, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_15 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_16, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_16, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_16 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_17, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_17, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_17 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_18, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_18, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_18 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_19, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_19, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_19 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    REPLACE(REPLACE(COUNTY, 'St.', 'St'), 'De Kalb', 'DeKalb') as COUNTY,
                    TRIM(SUBSTR(VOTER_HISTORY_20, 0, 10))                      AS ELECTION_DATE,
                    UPPER(TRIM(SUBSTR(VOTER_HISTORY_20, 12)))                  AS ELECTION_TYPE
             FROM DBT_VOTER_DATA.raw.MISSOURI_VOTERS_2023_10_10
             WHERE VOTER_HISTORY_20 IS NOT NULL)
SELECT 'MO'                                 AS STATE_CODE,
       COUNTY                               AS COUNTY_CODE,
       TO_DATE(ELECTION_DATE, 'MM/DD/YYYY') AS ELECTION_DATE,
       VOTER_ID                             AS VOTER_ID,
       ELECTION_TYPE                        AS ELECTION_TYPE,
       NULL                                 AS PARTY_VOTED,
       'UNKNOWN'                            AS VOTING_METHOD,
       NULL                                 AS DATE_OF_VOTING,
       TRUE                                 AS WAS_VOTE_COUNTED,
       NULL::object                         AS ADDITIONAL_DATA
FROM MO_HISTORY
WHERE ELECTION_TYPE IN (
                        'GENERAL', 'PRESIDENTIAL PRIMARY', 'RUNOFF', 'PRIMARY'
    )