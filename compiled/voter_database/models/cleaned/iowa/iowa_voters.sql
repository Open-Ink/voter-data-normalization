SELECT '2023-12-14 '::date                          as FILE_DATE,
       'IA'                                         as STATE_CODE,
       VOTERS.COUNTY                                as COUNTY_CODE,
       VOTERS.REGN_NUM                              AS VOTER_ID,
       NULL                                         AS PREFIX,
       VOTERS.FIRST_NAME                            AS FIRST_NAME,
       VOTERS.MIDDLE_NAME                           AS MIDDLE_NAME,
       VOTERS.LAST_NAME                             AS LAST_NAME,
       VOTERS.NAME_SUFFIX                           AS NAME_SUFFIX,
       COALESCE(PUBLIC.MERGE_ADDRESS_ELEMENTS([
           VOTERS.HOUSE_NUM,
           VOTERS.HOUSE_SUFFIX,
           VOTERS.PRE_DIR,
           VOTERS.STREET_NAME,
           VOTERS.STREET_TYPE,
           VOTERS.POST_DIR
           ]), ADDRESS_NON_STD)                     AS RESIDENCE_ADDRESS_LINE_1,
       PUBLIC.MERGE_ADDRESS_ELEMENTS([
           VOTERS.UNIT_TYPE,
           VOTERS.UNIT_NUM
           ])                                       AS RESIDENCE_ADDRESS_LINE_2,
       CITY                                         AS RESIDENCE_ADDRESS_CITY,
       COALESCE(STATE, 'IA')                        AS RESIDENCE_ADDRESS_STATE,
       ZIP_CODE                                     AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                        AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR(TO_DATE(BIRTHDATE, 'MM/DD/YYYY'))       AS BIRTH_YEAR,
       MONTH(TO_DATE(BIRTHDATE, 'MM/DD/YYYY'))      AS BIRTH_MONTH,
       DAYOFMONTH(TO_DATE(BIRTHDATE, 'MM/DD/YYYY')) AS BIRTH_DATE,
       CASE
           WHEN VOTERSTATUS = 'Active' THEN 'A'
           WHEN VOTERSTATUS = 'Inactive' THEN 'I'
           WHEN VOTERSTATUS = 'Pending' THEN 'P'
           ELSE NULL
           END                                      as VOTER_STATUS,
       TRY_TO_DATE(EFF_REGN_DATE, 'MM/DD/YYYY')     AS REGISTRATION_DATE,
       NULL                                         AS LAST_VOTED_DATE,
       PARTY                                        AS LAST_PARTY_VOTED,
       VOTERS.CONGRESSIONAL                         AS CONGRESSIONAL_DISTRICT,
       VOTERS.STATE_SENATE                          AS STATE_SENATE_DISTRICT,
       STATE_HOUSE                                  AS STATE_HOUSE_DISTRICT,
       JUDICIAL                                     AS JUDICIAL_DISTRICT,
       NULL                                         AS COUNTY_COMMISSION_DISTRICT,
       SCHOOL                                       AS SCHOOL_BOARD_DISTRICT,
       CITY_COUNCIL_WARD                            AS CITY_COUNCIL_DISTRICT,
       PRECINCT                                     AS COUNTY_PRECINCT,
       NULL                                         AS MUNICIPAL_PRECINCT,
       NULL                                         AS RACE,
       CASE
           WHEN GENDER = 'Male' THEN 'M'
           WHEN GENDER = 'Female' THEN 'F'
           END                                      AS GENDER,
       MAILING_ADDRESS                              AS MAILING_ADDRESS_LINE_1,
       NULL                                         AS MAILING_ADDRESS_LINE_2,
       NULL                                         AS MAILING_LINE_3,
       MAILING_ADDRESS_CITY                         AS MAILING_CITY,
       MAILING_ADDRESS_STATE                        AS MAILING_STATE,
       MAILING_ADDRESS_ZIP_CODE                     AS MAILING_ZIPCODE,
       NULL                                         AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.IOWA_VOTERS_2023_12_14 VOTERS
WHERE VOTERSTATUS IN ('Active', 'Inactive', 'Pending')
  AND NOT (REGN_NUM = '500201357' AND MAILING_ADDRESS = '1420 MULBERRY 130')