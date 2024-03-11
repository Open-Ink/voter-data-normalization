WITH DC_VOTERS AS (SELECT DISTINCT '2023-11-06'::date                                 as FILE_DATE,
                                   'KS'                                               as STATE_CODE,
                                   'DISTRICT OF COLUMBIA'                             as COUNTY_CODE,
                                   NULL                                               AS PREFIX,
                                   FIRST_NAME                                         AS FIRST_NAME,
                                   MIDDLE_NAME                                        AS MIDDLE_NAME,
                                   LAST_NAME                                          AS LAST_NAME,
                                   NAME_STYLE                                         AS NAME_SUFFIX,
                                   MERGE_ADDRESS_ELEMENTS([
                                       STREET_NUMBER,
                                       STREET_NUMBER_SUFFIX,
                                       STREET_NAME,
                                       STREET_TYPE,
                                       STREET_DIR_SUFFIX
                                       ])                                             AS RESIDENCE_ADDRESS_LINE_1,
                                   MERGE_ADDRESS_ELEMENTS([
                                       UNIT_TYPE,
                                       APARTMENT_NUMBER
                                       ])                                             AS RESIDENCE_ADDRESS_LINE_2,
                                   DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10.CITY_NAME AS RESIDENCE_ADDRESS_CITY,
                                   'DC'                                               AS RESIDENCE_ADDRESS_STATE,
                                   ZIP_CODE                                           AS RESIDENCE_ADDRESS_ZIPCODE,
                                   'USA'                                              AS RESIDENCE_ADDRESS_COUNTRY,
                                   NULL                                               AS BIRTH_YEAR,
                                   NULL                                               AS BIRTH_MONTH,
                                   NULL                                               AS BIRTH_DATE,
                                   CASE
                                       WHEN VOTER_STATUS = 'ACT' THEN 'A'
                                       ELSE NULL END                                  as VOTER_STATUS,
                                   TO_DATE(REGISTRATION_DATE, 'MM/DD/YYYY')           AS REGISTRATION_DATE,
                                   NULL                                               AS LAST_VOTED_DATE,
                                   PARTY                                              AS LAST_PARTY_VOTED,
                                   NULL                                               AS CONGRESSIONAL_DISTRICT,
                                   NULl                                               AS STATE_SENATE_DISTRICT,
                                   NULl                                               AS STATE_HOUSE_DISTRICT,
                                   NULl                                               AS JUDICIAL_DISTRICT,
                                   NULL                                               AS COUNTY_COMMISSION_DISTRICT,
                                   NULl                                               AS SCHOOL_BOARD_DISTRICT,
                                   NULL                                               AS CITY_COUNCIL_DISTRICT,
                                   PRECINCT                                           AS COUNTY_PRECINCT,
                                   NULL                                               AS MUNICIPAL_PRECINCT,
                                   NULL                                               AS RACE,
                                   NULL                                               AS GENDER,
                                   NULL                                               AS MAILING_ADDRESS_LINE_1,
                                   NULL                                               AS MAILING_ADDRESS_LINE_2,
                                   NULL                                               AS MAILING_LINE_3,
                                   NULL                                               AS MAILING_CITY,
                                   NULL                                               AS MAILING_STATE,
                                   NULL                                               AS MAILING_ZIPCODE,
                                   NULL                                               AS MAILING_COUNTRY
                   FROM DBT_VOTER_DATA.raw.DISTRICT_OF_COLUMBIA_RAW_DATA_2023_03_10
SELECT BASE64_ENCODE(UPPER(REPLACE(MERGE_ADDRESS_ELEMENTS([
    FIRST_NAME,
    MIDDLE_NAME,
    LAST_NAME,
    NAME_SUFFIX,
    RESIDENCE_ADDRESS_LINE_1,
    RESIDENCE_ADDRESS_LINE_2]), ' ', ''))) AS VOTER_ID,
       *
FROM DC_VOTERS
QUALIFY ROW_NUMBER() OVER (PARTITION BY VOTER_ID ORDER BY REGISTRATION_DATE DESC) = 1