WITH VOTER_HISTORY
         AS (SELECT WS_TOWN_ID,
                    WS_ID_VOTER       AS VOTER_ID,
                    WS_ELECT_1_TYPE   AS ELECTION_TYPE,
                    WS_ELECT_1_DT     AS ELECTION_DATE,
                    WS_ELECT_1_ABSTEE AS IS_ABSENTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_1_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_2_TYPE, WS_ELECT_2_DT, WS_ELECT_2_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_2_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_3_TYPE, WS_ELECT_3_DT, WS_ELECT_3_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_3_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_4_TYPE, WS_ELECT_4_DT, WS_ELECT_4_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_4_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_5_TYPE, WS_ELECT_5_DT, WS_ELECT_5_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_5_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_6_TYPE, WS_ELECT_6_DT, WS_ELECT_6_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_6_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_7_TYPE, WS_ELECT_7_DT, WS_ELECT_7_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_7_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_8_TYPE, WS_ELECT_8_DT, WS_ELECT_8_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_8_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_9_TYPE, WS_ELECT_9_DT, WS_ELECT_9_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_9_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_10_TYPE, WS_ELECT_10_DT, WS_ELECT_10_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_10_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_11_TYPE, WS_ELECT_11_DT, WS_ELECT_11_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_11_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_12_TYPE, WS_ELECT_12_DT, WS_ELECT_12_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_12_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_13_TYPE, WS_ELECT_13_DT, WS_ELECT_13_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_13_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_14_TYPE, WS_ELECT_14_DT, WS_ELECT_14_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_14_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_15_TYPE, WS_ELECT_15_DT, WS_ELECT_15_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_15_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_16_TYPE, WS_ELECT_16_DT, WS_ELECT_16_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_16_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_17_TYPE, WS_ELECT_17_DT, WS_ELECT_17_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_17_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_18_TYPE, WS_ELECT_18_DT, WS_ELECT_18_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_18_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_19_TYPE, WS_ELECT_19_DT, WS_ELECT_19_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_19_TYPE IS NOT NULL
             UNION
             SELECT WS_TOWN_ID, WS_ID_VOTER, WS_ELECT_20_TYPE, WS_ELECT_20_DT, WS_ELECT_20_ABSTEE
             FROM DBT_VOTER_DATA.raw.connecticut_voters_2023_10_12
             WHERE WS_ELECT_20_TYPE IS NOT NULL)
SELECT 'CT'                                            AS STATE_CODE,
       VOTER_ID                                        AS VOTER_ID,
       T.COUNTY_NAME                                   AS COUNTY_CODE,
       TO_DATE(ELECTION_DATE, 'MM/DD/YYYY')            AS ELECTION_DATE,
       CASE
           WHEN ELECTION_TYPE = 'P' THEN 'PRIMARY'
           WHEN ELECTION_TYPE = 'E' THEN 'GENERAL' END AS ELECTION_TYPE,
       NULL                                            AS PARTY_VOTED,
       CASE
           WHEN IS_ABSENTEE IN ('N') THEN ' STANDARD'
           WHEN IS_ABSENTEE IN ('Y') THEN ' ABSENTEE'
           END                                         AS VOTING_METHOD,
       NULL                                            AS DATE_OF_VOTING,
       TRUE                                            AS WAS_VOTE_COUNTED,
       NULL:null                                       AS ADDITIONAL_DATA
FROM VOTER_HISTORY VH
         INNER JOIN DBT_VOTER_DATA.cleaned.connecticut_towns T
                    ON VH.WS_TOWN_ID = T.TOWN_ID
WHERE ELECTION_TYPE IN ('P', 'E')