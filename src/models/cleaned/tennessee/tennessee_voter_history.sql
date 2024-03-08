WITH VOTER_HISTORY AS
         (SELECT COUNTY,
                 VOTER_ID,
                 NOV_22_HOW                   as VOTING_METHOD,
                 NULL                         AS PARTY_VOTED,
                 'GENERAL'                    as ELECTION_TYPE,
                 NEXT_DAY('2022-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_22_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_22_HOW,
                 AUG_22_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2022-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_22_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_20_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2020-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_20_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_20_HOW,
                 AUG_20_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2020-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_20_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 MAR_20_HOW,
                 MAR_20_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2020-03-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE MAR_20_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_18_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2018-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_18_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_18_HOW,
                 AUG_18_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2018-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_16_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2016-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_16_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_16_HOW,
                 AUG_16_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2016-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_16_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 MAR_16_HOW,
                 MAR_16_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2016-03-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE MAR_16_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_14_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2014-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_14_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_14_HOW,
                 AUG_14_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2014-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_12_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2012-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_12_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_12_HOW,
                 AUG_12_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2012-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_12_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 MAR_12_HOW,
                 MAR_12_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2012-03-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE MAR_12_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_10_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2010-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_10_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_10_HOW,
                 AUG_10_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2010-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_10_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_08_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2008-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_08_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_08_HOW,
                 AUG_08_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2008-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_08_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 FEB_08_HOW,
                 FEB_08_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2008-03-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE FEB_08_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_06_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2006-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_06_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_06_HOW,
                 AUG_06_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2006-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_06_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_04_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2004-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_04_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_04_HOW,
                 AUG_04_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2004-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_04_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 FEB_04_HOW,
                 FEB_04_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2004-03-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE FEB_04_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_02_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2002-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_02_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_02_HOW,
                 AUG_02_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2002-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_02_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 NOV_00_HOW,
                 NULL,
                 'GENERAL',
                 NEXT_DAY('2000-11-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE NOV_00_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 AUG_00_HOW,
                 AUG_00_PARTY,
                 'PRIMARY',
                 NEXT_DAY('2000-08-01', 'th') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE AUG_00_HOW IS NOT NULL
          UNION
          SELECT COUNTY,
                 VOTER_ID,
                 MAR_00_HOW,
                 MAR_00_PARTY,
                 'PRESIDENTIAL PRIMARY',
                 NEXT_DAY('2000-03-01', 'tu') AS ELECTION_DATE
          FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }}
          WHERE MAR_00_HOW IS NOT NULL)
SELECT 'TN'             AS STATE_CODE,
       C.COUNTY_NAME        AS COUNTY_CODE,
       VH.VOTER_ID      AS VOTER_ID,
       VH.ELECTION_DATE AS ELECTION_DATE,
       VH.PARTY_VOTED   AS PARTY_VOTED,
       NULL             AS VOTING_METHOD,
       NULL             AS DATE_OF_VOTING,
       TRUE             AS WAS_VOTE_COUNTED,
       NULL::object     AS ADDITIONAL_DATA,
       VH.ELECTION_TYPE AS ELECTION_TYPE
FROM VOTER_HISTORY VH
         INNER JOIN {{ ref("tennessee_counties") }} C ON VH.COUNTY = C.COUNTY_ID