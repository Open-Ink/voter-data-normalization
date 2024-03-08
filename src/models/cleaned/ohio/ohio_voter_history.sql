WITH OHIO_VOTER_HISTORY AS (SELECT COUNTY_NUMBER,
                                   SOS_VOTERID,
                                   ELECTION_CODE,
                                   VOTE_TYPE_CODE
                            FROM {{ source('raw', 'OHIO_VOTERS_2023_09_25') }} UNPIVOT (VOTE_TYPE_CODE FOR ELECTION_CODE IN (PRIMARY_2000_03_07,
                                GENERAL_2000_11_07,
                                SPECIAL_2001_05_08,
                                GENERAL_2001_11_06,
                                PRIMARY_2002_05_07,
                                GENERAL_2002_11_05,
                                SPECIAL_2003_05_06,
                                GENERAL_2003_11_04,
                                PRIMARY_2004_03_02,
                                GENERAL_2004_11_02,
                                SPECIAL_2005_02_08,
                                PRIMARY_2005_05_03,
                                PRIMARY_2005_09_13,
                                GENERAL_2005_11_08,
                                SPECIAL_2006_02_07,
                                PRIMARY_2006_05_02,
                                GENERAL_2006_11_07,
                                PRIMARY_2007_05_08,
                                PRIMARY_2007_09_11,
                                GENERAL_2007_11_06,
                                PRIMARY_2007_11_06,
                                GENERAL_2007_12_11,
                                PRIMARY_2008_03_04,
                                PRIMARY_2008_10_14,
                                GENERAL_2008_11_04,
                                GENERAL_2008_11_18,
                                PRIMARY_2009_05_05,
                                PRIMARY_2009_09_08,
                                PRIMARY_2009_09_15,
                                PRIMARY_2009_09_29,
                                GENERAL_2009_11_03,
                                PRIMARY_2010_05_04,
                                PRIMARY_2010_07_13,
                                PRIMARY_2010_09_07,
                                GENERAL_2010_11_02,
                                PRIMARY_2011_05_03,
                                PRIMARY_2011_09_13,
                                GENERAL_2011_11_08,
                                PRIMARY_2012_03_06,
                                GENERAL_2012_11_06,
                                PRIMARY_2013_05_07,
                                PRIMARY_2013_09_10,
                                PRIMARY_2013_10_01,
                                GENERAL_2013_11_05,
                                PRIMARY_2014_05_06,
                                GENERAL_2014_11_04,
                                PRIMARY_2015_05_05,
                                PRIMARY_2015_09_15,
                                GENERAL_2015_11_03,
                                PRIMARY_2016_03_15,
                                GENERAL_2016_06_07,
                                PRIMARY_2016_09_13,
                                GENERAL_2016_11_08,
                                PRIMARY_2017_05_02,
                                PRIMARY_2017_09_12,
                                GENERAL_2017_11_07,
                                PRIMARY_2018_05_08,
                                GENERAL_2018_08_07,
                                GENERAL_2018_11_06,
                                PRIMARY_2019_05_07,
                                PRIMARY_2019_09_10,
                                GENERAL_2019_11_05,
                                PRIMARY_2020_03_17,
                                GENERAL_2020_11_03,
                                PRIMARY_2021_05_04,
                                PRIMARY_2021_08_03,
                                PRIMARY_2021_09_14,
                                GENERAL_2021_11_02,
                                PRIMARY_2022_05_03,
                                PRIMARY_2022_08_02,
                                GENERAL_2022_11_08,
                                SPECIAL_2023_02_28,
                                PRIMARY_2023_05_02,
                                SPECIAL_2023_08_08
                                )))
SELECT 'OH'                                                        AS STATE_CODE,
       COUNTY.county_name                                          AS COUNTY_CODE,
       SOS_VOTERID                                                 AS VOTER_ID,
       ELECTION_CODE,
       SUBSTR(ELECTION_CODE, 0, CHARINDEX('_', ELECTION_CODE) - 1) AS ELECTION_TYPE,
       TO_DATE(
               REPLACE(
                       SUBSTR(ELECTION_CODE, CHARINDEX('_', ELECTION_CODE) + 1),
                       '_',
                       '-'),
               'YYYY-MM-DD')                                       AS ELECTION_DATE,
       CASE
           WHEN VOTE_TYPE_CODE = 'C' THEN 'Constitution Party'
           WHEN VOTE_TYPE_CODE = 'D' THEN 'Democrat Party'
           WHEN VOTE_TYPE_CODE = 'E' THEN 'Reform Party'
           WHEN VOTE_TYPE_CODE = 'G' THEN 'Green Party'
           WHEN VOTE_TYPE_CODE = 'L' THEN 'Libertarian Party'
           WHEN VOTE_TYPE_CODE = 'N' THEN 'Natural Law Party'
           WHEN VOTE_TYPE_CODE = 'R' THEN 'Republican Party'
           WHEN VOTE_TYPE_CODE = 'S' THEN 'Socialist Party'
           WHEN VOTE_TYPE_CODE = 'X' THEN 'Unaffiliated' END       AS PARTY_VOTED,
       NULL                                                        AS DATE_OF_VOTING,
       TRUE                                                        AS WAS_VOTE_COUNTED,
       NULL::object                                                AS ADDITIONAL_DATA,
       'UNKNOWN'                                                   AS VOTING_METHOD
FROM OHIO_VOTER_HISTORY
         INNER JOIN {{ ref("ohio_counties") }} COUNTY ON COUNTY_NUMBER = COUNTY.county_id