SELECT '2023-11-16'::date                                                        as FILE_DATE,
       'RI'                                                                      as STATE_CODE,
       NULL                                                                      as COUNTY_CODE,
       VOTERS.VOTER_ID                                                           AS VOTER_ID,
       VOTERS.PREFIX                                                             AS PREFIX,
       VOTERS.FIRST_NAME                                                         AS FIRST_NAME,
       VOTERS.MIDDLE_NAME                                                        AS MIDDLE_NAME,
       VOTERS.LAST_NAME                                                          AS LAST_NAME,
       VOTERS.SUFFIX                                                             AS NAME_SUFFIX,
       MERGE_ADDRESS_ELEMENTS([
           VOTERS.STREET_NUMBER,
           VOTERS.STREET_NAME,
           VOTERS.STREET_NAME_2
           ])                                                                    AS RESIDENCE_ADDRESS_LINE_1,
       VOTERS.UNIT                                                               AS RESIDENCE_ADDRESS_LINE_2,
       CITY                                                                      AS RESIDENCE_ADDRESS_CITY,
       COALESCE(STATE, 'RI')                                                     AS RESIDENCE_ADDRESS_STATE,
       ZIP4_CODE                                                                 AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                                                     AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR_OF_BIRTH                                                             AS BIRTH_YEAR,
       NULL                                                                      AS BIRTH_MONTH,
       NULL                                                                      AS BIRTH_DATE,
       STATUS                                                                    as VOTER_STATUS,
       TO_DATE(REGISTRATION_DATE, 'MM/DD/YYYY')                                  AS REGISTRATION_DATE,
       NULL                                                                      AS LAST_VOTED_DATE,
       PARTY_CODE                                                                AS LAST_PARTY_VOTED,
       CONGRESSIONAL_DISTRICT                                                    AS CONGRESSIONAL_DISTRICT,
       STATE_SENATE_DISTRICT                                                     AS STATE_SENATE_DISTRICT,
       STATE_REP_DISTRICT                                                        AS STATE_HOUSE_DISTRICT,
       NULL                                                                      AS JUDICIAL_DISTRICT,
       NULL                                                                      AS COUNTY_COMMISSION_DISTRICT,
       NULL                                                                      AS SCHOOL_BOARD_DISTRICT,
       NULL                                                                      AS CITY_COUNCIL_DISTRICT,
       PRECINCT                                                                  AS COUNTY_PRECINCT,
       NULL                                                                      AS MUNICIPAL_PRECINCT,
       NULL                                                                      AS RACE,
       CASE WHEN GENDER = 'N' THEN 'U' ELSE GENDER END                           AS GENDER,
       MERGE_ADDRESS_ELEMENTS([
           MAILING_STREET_NUMBER, MAILING_STREET_NAME_1, MAILING_STREET_NAME_2]) AS MAILING_ADDRESS_LINE_1,
       MAILING_UNIT                                                              AS MAILING_ADDRESS_LINE_2,
       NULL                                                                      AS MAILING_LINE_3,
       MAILING_CITY                                                              AS MAILING_CITY,
       MAILING_STATE                                                             AS MAILING_STATE,
       MAILING_ZIP_CODE                                                          AS MAILING_ZIPCODE,
       MAILING_COUNTRY                                                           AS MAILING_COUNTRY
FROM {{ source('raw', 'RHODE_ISLAND_VOTERS_2023_11_16') }} VOTERS


