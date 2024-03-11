WITH CTE_VH AS (SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'GENERAL'                AS ELECTION_TYPE,
                       '2022-11-08'::date       AS ELECTION_DATE,
                       GENERAL_2022_PARTY_VOTED AS PARTY_VOTED,
                       GENERAL_2022_HOW_VOTED   AS VOTING_METHOD,
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE GENERAL_2022 IS NOT NULL
                UNION ALL
                SELECT COUNTY                          AS COUNTY_CODE,
                       VOTERID                         AS VOTER_ID,
                       'PRIMARY RUNOFFS'               AS ELECTION_TYPE,
                       '2022-06-21'::date              AS ELECTION_DATE,
                       PRIMARY_RUNOFF_2022_PARTY_VOTED AS PARTY_VOTED,
                       PRIMARY_RUNOFF_2022_HOW_VOTED   AS VOTING_METHOD,
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PRIMARY_RUNOFF_2022 IS NOT NULL
                UNION ALL
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'PRIMARY'                AS ELECTION_TYPE,
                       '2022-05-24'::date       AS ELECTION_DATE,
                       PRIMARY_2022_PARTY_VOTED AS PARTY_VOTED,
                       PRIMARY_2022_HOW_VOTED   AS VOTING_METHOD,
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PRIMARY_2022 IS NOT NULL
                UNION ALL
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'GENERAL'                AS ELECTION_TYPE,
                       '2020-11-03'::date       AS ELECTION_DATE,
                       GENERAL_2020_PARTY_VOTED AS PARTY_VOTED,
                       GENERAL_2020_HOW_VOTED   AS VOTING_METHOD,
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE GENERAL_2020 IS NOT NULL
                UNION ALL
                SELECT COUNTY                          AS COUNTY_CODE,
                       VOTERID                         AS VOTER_ID,
                       'PRIMARY RUNOFF'                AS ELECTION_TYPE,
                       '2020-03-31'::date              AS ELECTION_DATE,
                       PRIMARY_RUNOFF_2020_PARTY_VOTED AS PARTY_VOTED,
                       PRIMARY_RUNOFF_2020_HOW_VOTED   AS VOTING_METHOD,
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PRIMARY_RUNOFF_2020 IS NOT NULL
                UNION ALL
                SELECT COUNTY                                AS COUNTY_CODE,
                       VOTERID                               AS VOTER_ID,
                       'PRIMARY'                             AS ELECTION_TYPE,
                       '2020-03-03'::date                    AS ELECTION_DATE,
                       PREFERENTIAL_PRIMARY_2020_PARTY_VOTED AS PARTY_VOTED,
                       PREFERENTIAL_PRIMARY_2020_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PREFERENTIAL_PRIMARY_2020 IS NOT NULL
                UNION ALL
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'GENERAL'                AS ELECTION_TYPE,
                       '2018-11-06'::date       AS ELECTION_DATE,
                       GENERAL_2018_PARTY_VOTED AS PARTY_VOTED,
                       GENERAL_2018_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE GENERAL_2018 IS NOT NULL
                UNION ALL
                SELECT COUNTY                                AS COUNTY_CODE,
                       VOTERID                               AS VOTER_ID,
                       'PREFERENTIAL PRIMARY'                AS ELECTION_TYPE,
                       '2018-05-22'::date                    AS ELECTION_DATE,
                       PREFERENTIAL_PRIMARY_2018_PARTY_VOTED AS PARTY_VOTED,
                       PREFERENTIAL_PRIMARY_2018_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PREFERENTIAL_PRIMARY_2018 IS NOT NULL
                UNION ALL
                SELECT COUNTY                          AS COUNTY_CODE,
                       VOTERID                         AS VOTER_ID,
                       'PRIMARY RUNOFFS'               AS ELECTION_TYPE,
                       '2018-05-22'::date              AS ELECTION_DATE,
                       PRIMARY_RUNOFF_2016_PARTY_VOTED AS PARTY_VOTED,
                       PRIMARY_RUNOFF_2016_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PRIMARY_RUNOFF_2016 IS NOT NULL
                UNION ALL
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'GENERAL'                AS ELECTION_TYPE,
                       '2016-11-08'::date       AS ELECTION_DATE,
                       GENERAL_2016_PARTY_VOTED AS PARTY_VOTED,
                       GENERAL_2016_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE GENERAL_2016 IS NOT NULL
                UNION ALL
                SELECT COUNTY                          AS COUNTY_CODE,
                       VOTERID                         AS VOTER_ID,
                       'PRIMARY RUNOFF'                AS ELECTION_TYPE,
                       '2016-03-22'::date              AS ELECTION_DATE,
                       PRIMARY_RUNOFF_2016_PARTY_VOTED AS PARTY_VOTED,
                       PRIMARY_RUNOFF_2016_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PRIMARY_RUNOFF_2016 IS NOT NULL
                UNION
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'PRIMARY'                AS ELECTION_TYPE,
                       '2016-11-08'::date       AS ELECTION_DATE,
                       PRIMARY_2016_PARTY_VOTED AS PARTY_VOTED,
                       PRIMARY_2016_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PRIMARY_2016 IS NOT NULL
                UNION ALL
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'GENERAL'                AS ELECTION_TYPE,
                       '2014-11-04'::date       AS ELECTION_DATE,
                       GENERAL_2014_PARTY_VOTED AS PARTY_VOTED,
                       GENERAL_2014_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE GENERAL_2014 IS NOT NULL
                UNION ALL
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'PRIMARY'                AS ELECTION_TYPE,
                       '2014-05-20'::date       AS ELECTION_DATE,
                       PRIMARY_2014_PARTY_VOTED AS PARTY_VOTED,
                       PRIMARY_2014_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PRIMARY_2014 IS NOT NULL
                UNION ALL
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'GENERAL'                AS ELECTION_TYPE,
                       '2012-11-06'::date       AS ELECTION_DATE,
                       GENERAL_2012_PARTY_VOTED AS PARTY_VOTED,
                       GENERAL_2012_HOW_VOTED   AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE GENERAL_2012 IS NOT NULL
                UNION ALL
                SELECT COUNTY                   AS COUNTY_CODE,
                       VOTERID                  AS VOTER_ID,
                       'PRIMARY'                AS ELECTION_TYPE,
                       '2012-05-22'::date       AS ELECTION_DATE,
                       PRIMARY_2012_PARTY_VOTED AS PARTY_VOTED,
                       PRIMARY_2012HOW_VOTED    AS VOTING_METHOD
                FROM DBT_VOTER_DATA.raw.ARKANSAS_VOTER_HISTORY_2023_09_01
                WHERE PRIMARY2012 IS NOT NULL)
SELECT 'AR'          AS STATE_CODE,
       VOTER_ID      AS VOTER_ID,
       COUNTY_CODE   AS COUNTY_CODE,
       ELECTION_DATE AS ELECTION_DATE,
       ELECTION_TYPE AS ELECTION_TYPE,
       PARTY_VOTED   AS PARTY_VOTED,
       CASE
           WHEN VOTING_METHOD IN ('P', 'F') THEN 'STANDARD'
           WHEN VOTING_METHOD = 'A' THEN 'ABSENTEE'
           WHEN VOTING_METHOD = 'E' THEN 'EARLY_VOTING'
           WHEN VOTING_METHOD in ('V', 'B', 'Y') THEN 'PROVISIONAL'
           END       AS VOTING_METHOD,
       NULL          AS DATE_OF_VOTING,
       TRUE          AS WAS_VOTE_COUNTED,
       NULL:null     AS ADDITIONAL_DATA
FROM CTE_VH