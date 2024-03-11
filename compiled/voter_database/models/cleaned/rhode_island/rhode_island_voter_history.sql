WITH RHODE_ISLAND_VOTER_VOTER_HISTORY
         AS (SELECT VOTER_ID,
                    ELECTION_1 AS ELECTION_TYPE,
                    TYPE_1     AS VOTE_TYPE,
                    PRECINCT_1,
                    dATE_1     AS ELECTION_DATE,
                    PARTY_1    AS PARTY_VOTED
             FROM DBT_VOTER_DATA.raw.RHODE_ISLAND_VOTER_HISTORY_2023_11_16
             WHERE ELECTION_1 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    ELECTION_2,
                    TYPE_2,
                    PRECINCT_2,
                    DATE_2,
                    PARTY_2
             FROM DBT_VOTER_DATA.raw.RHODE_ISLAND_VOTER_HISTORY_2023_11_16
             WHERE ELECTION_2 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    ELECTION_3,
                    TYPE_3,
                    PRECINCT_3,
                    dATE_3,
                    PARTY_3
             FROM DBT_VOTER_DATA.raw.RHODE_ISLAND_VOTER_HISTORY_2023_11_16
             WHERE ELECTION_3 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    ELECTION_4,
                    TYPE_4,
                    PRECINCT_4,
                    DATE_4,
                    PARTY_4
             FROM DBT_VOTER_DATA.raw.RHODE_ISLAND_VOTER_HISTORY_2023_11_16
             WHERE ELECTION_4 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    ELECTION_5,
                    TYPE_5,
                    PRECINCT_5,
                    DATE_5,
                    PARTY_5
             FROM DBT_VOTER_DATA.raw.RHODE_ISLAND_VOTER_HISTORY_2023_11_16
             WHERE ELECTION_5 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    ELECTION_6,
                    TYPE_6,
                    PRECINCT_6,
                    DATE_6,
                    PARTY_6
             FROM DBT_VOTER_DATA.raw.RHODE_ISLAND_VOTER_HISTORY_2023_11_16
             WHERE ELECTION_6 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    ELECTION_7,
                    TYPE_7,
                    PRECINCT_7,
                    DATE_7,
                    PARTY_7
             FROM DBT_VOTER_DATA.raw.RHODE_ISLAND_VOTER_HISTORY_2023_11_16
             WHERE ELECTION_7 IS NOT NULL
             UNION
             SELECT VOTER_ID,
                    ELECTION_8,
                    TYPE_8,
                    PRECINCT_8,
                    dATE_8,
                    PARTY_8
             FROM DBT_VOTER_DATA.raw.RHODE_ISLAND_VOTER_HISTORY_2023_11_16
             WHERE ELECTION_8 IS NOT NULL)
SELECT 'RI'                                                               AS STATE_CODE,
       V.COUNTY_CODE                                                      AS COUNTY_CODE,
       VH.VOTER_ID                                                        AS VOTER_ID,
       CASE
           WHEN VH.ELECTION_TYPE = 'STATEWIDE GENERAL ELECTION' THEN 'GENERAL'
           WHEN VH.ELECTION_TYPE = 'STATEWIDE PRIMARY' THEN 'PRIMARY'
           WHEN VH.ELECTION_TYPE = 'SPECIAL ELECTIONS' THEN 'SPECIAL' END AS ELECTION_TYPE,
       TO_DATE(VH.ELECTION_DATE, 'MM/DD/YYYY')                            AS ELECTION_DATE,
       PARTY_VOTED                                                        AS PARTY_VOTED,
       CASE
           WHEN VOTE_TYPE = 'E' THEN 'EARLY_VOTING'
           WHEN VOTE_TYPE = 'M' THEN 'ABSENTEE'
           WHEN VOTE_TYPE = 'R' THEN 'REGULAR'
           END                                                            AS VOTING_METHOD,
       NULL                                                               AS DATE_OF_VOTING,
       TRUE                                                               AS WAS_VOTE_COUNTED,
       NULL::object                                                       AS ADDITIONAL_DATA
FROM RHODE_ISLAND_VOTER_VOTER_HISTORY VH
         INNER JOIN DBT_VOTER_DATA.cleaned.rhode_island_voters V
                    ON V.VOTER_ID = VH.VOTER_ID
WHERE ELECTION_TYPE IN ('STATEWIDE PRIMARY', 'STATEWIDE GENERAL ELECTION', 'SPECIAL ELECTIONS')