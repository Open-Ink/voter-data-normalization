WITH DC_HISTORY AS
         (SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 GENERAL_ELECTION_NOV_08_2022 AS VOTING_METHOD,
                 '2022-11-08'                 AS ELECTION_DATE,
                 'GENERAL'                    AS ELECTION_TYPE
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE GENERAL_ELECTION_NOV_08_2022 IS NOT NULl
            AND GENERAL_ELECTION_NOV_08_2022 != 'N'
            AND GENERAL_ELECTION_NOV_08_2022 != 'X'
          UNION
          SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 PRIMARY_ELECTION_JUN_21_2022 AS VOTING_METHOD,
                 '2022-06-21'                 AS ELECTION_DATE,
                 'PRIMARY'
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE PRIMARY_ELECTION_JUN_21_2022 IS NOT NULl
            AND PRIMARY_ELECTION_JUN_21_2022 != 'N'
            AND PRIMARY_ELECTION_JUN_21_2022 != 'X'
          UNION
          SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 DC_PRESIDENTIAL_GENERAL_ELECTION_NOV_03_2020 AS VOTING_METHOD,
                 '2023-11-03'                                 AS ELECTION_DATE,
                 'GENERAL'
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE DC_PRESIDENTIAL_GENERAL_ELECTION_NOV_03_2020 IS NOT NULl
            AND DC_PRESIDENTIAL_GENERAL_ELECTION_NOV_03_2020 != 'N'
            AND DC_PRESIDENTIAL_GENERAL_ELECTION_NOV_03_2020 != 'X'
          UNION
          SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 DC_PRESIDENTIAL_PRIMARY_ELECTION_JUN_02_2020 AS VOTING_METHOD,
                 '2020-06-02'                                 AS ELECTION_DATE,
                 'PRESIDENTIAL PRIMARY'
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE DC_PRESIDENTIAL_PRIMARY_ELECTION_JUN_02_2020 IS NOT NULl
            AND DC_PRESIDENTIAL_PRIMARY_ELECTION_JUN_02_2020 != 'N'
            AND DC_PRESIDENTIAL_PRIMARY_ELECTION_JUN_02_2020 != 'X'
          UNION
          SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 DC_GENERAL_ELECTION_NOV_06_2018 AS VOTING_METHOD,
                 '2018-11-06'                    AS ELECTION_DATE,
                 'GENERAL'
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE DC_GENERAL_ELECTION_NOV_06_2018 IS NOT NULl
            AND DC_GENERAL_ELECTION_NOV_06_2018 != 'N'
            AND DC_GENERAL_ELECTION_NOV_06_2018 != 'X'
          UNION
          SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 DC_PRESIDENTIAL_GENERAL_ELECTION_NOV_08_2016 AS VOTING_METHOD,
                 '2016-11-08'                                 AS ELECTION_DATE,
                 'GENERAL'
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE DC_PRESIDENTIAL_GENERAL_ELECTION_NOV_08_2016 IS NOT NULl
            AND DC_PRESIDENTIAL_GENERAL_ELECTION_NOV_08_2016 != 'N'
            AND DC_PRESIDENTIAL_GENERAL_ELECTION_NOV_08_2016 != 'X'
          UNION
          SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 PRESIDENTIAL_PRIMARY_ELECTION_JUN_14_2016 AS VOTING_METHOD,
                 '2016-06-14'                              AS ELECTION_DATE,
                 'PRESIDENTIAL PRIMARY'
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE PRESIDENTIAL_PRIMARY_ELECTION_JUN_14_2016 IS NOT NULl
            AND PRESIDENTIAL_PRIMARY_ELECTION_JUN_14_2016 != 'N'
            AND PRESIDENTIAL_PRIMARY_ELECTION_JUN_14_2016 != 'X'
          UNION
          SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 DISTRICT_OF_COLUMBIA_GENERAL_ELECTION_NOV_04_2014 AS VOTING_METHOD,
                 '2014-11-04'                                      AS ELECTION_DATE,
                 'GENERAL'
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE DISTRICT_OF_COLUMBIA_GENERAL_ELECTION_NOV_04_2014 IS NOT NULl
            AND DISTRICT_OF_COLUMBIA_GENERAL_ELECTION_NOV_04_2014 != 'N'
            AND DISTRICT_OF_COLUMBIA_GENERAL_ELECTION_NOV_04_2014 != 'X'
          UNION
          SELECT FIRST_NAME,
                 MIDDLE_NAME,
                 LAST_NAME,
                 NAME_STYLE,
                 STREET_NUMBER,
                 STREET_NUMBER_SUFFIX,
                 STREET_NAME,
                 STREET_TYPE,
                 STREET_DIR_SUFFIX,
                 UNIT_TYPE,
                 APARTMENT_NUMBER,
                 DISTRICT_OF_COLUMBIA_GENERAL_ELECTION_NOV_06_2012 AS VOTING_METHOD,
                 '2012-11-06'                                      AS ELECTION_DATE,
                 'GENERAL'
          FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
          WHERE DISTRICT_OF_COLUMBIA_GENERAL_ELECTION_NOV_06_2012 IS NOT NULl
            AND DISTRICT_OF_COLUMBIA_GENERAL_ELECTION_NOV_06_2012 != 'N'
            AND DISTRICT_OF_COLUMBIA_GENERAL_ELECTION_NOV_06_2012 != 'X'),
     DC_WITH_ADDRESS AS (SELECT *,
                                MERGE_ADDRESS_ELEMENTS([
                                    STREET_NUMBER,
                                    STREET_NUMBER_SUFFIX,
                                    STREET_NAME,
                                    STREET_TYPE,
                                    STREET_DIR_SUFFIX
                                    ]) AS RESIDENCE_ADDRESS_LINE_1,
                                MERGE_ADDRESS_ELEMENTS([
                                    UNIT_TYPE,
                                    APARTMENT_NUMBER
                                    ]) AS RESIDENCE_ADDRESS_LINE_2
                         FROM DC_HISTORY),
     DC_WITH_IDS AS
         (SELECT BASE64_ENCODE(UPPER(REPLACE(MERGE_ADDRESS_ELEMENTS([
             FIRST_NAME,
             MIDDLE_NAME,
             LAST_NAME,
             NAME_STYLE,
             RESIDENCE_ADDRESS_LINE_1,
             RESIDENCE_ADDRESS_LINE_2]), ' ', ''))) AS VOTER_ID,
                 VOTING_METHOD,
                 ELECTION_TYPE,
                 ELECTION_DATE
          FROM DC_WITH_ADDRESS)
SELECT 'DC'                                          AS STATE_CODE,
       VOTER_ID                                      AS VOTER_ID,
       'DISTRICT OF COLUMBIA'                        AS COUNTY_CODE,
       ELECTION_DATE                                 AS ELECTION_DATE,
       ELECTION_TYPE                                 AS ELECTION_TYPE,
       NULL                                          AS PARTY_VOTED,
       CASE
           WHEN VOTING_METHOD IN ('Y', 'Z') THEN 'STANDARD'
           WHEN VOTING_METHOD IN ('A', 'B') THEN 'ABSENTEE'
           WHEN VOTING_METHOD IN ('E', 'F') THEN 'EARLY_VOTING'
           END                                       AS VOTING_METHOD,
       NULL                                          AS DATE_OF_VOTING,
       IFF(VOTING_METHOD IN ('B', 'P'), FALSE, TRUE) AS WAS_VOTE_COUNTED,
       NULL:null                                     AS ADDITIONAL_DATA
FROM DC_WITH_IDS
QUALIFY ROW_NUMBER() OVER (PARTITION BY VOTER_ID, ELECTION_DATE, VOTING_METHOD ORDER BY ELECTION_DATE DESC) = 1