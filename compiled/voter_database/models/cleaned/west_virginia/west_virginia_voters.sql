SELECT '2023-11-06'::date              as FILE_DATE,
       'WV'                            as STATE_CODE,
       COUNTY_NAME                     as COUNTY_CODE,
       ID_VOTER                        AS VOTER_ID,
       NULL                            AS PREFIX,
       FIRST_NAME                      AS FIRST_NAME,
       MID                             AS MIDDLE_NAME,
       LAST_NAME                       AS LAST_NAME,
       SUFFIX                          AS NAME_SUFFIX,
       PUBLIC.MERGE_ADDRESS_ELEMENTS([
           HOUSE_NO,
           STREET
           ])                          AS RESIDENCE_ADDRESS_LINE_1,
       COALESCE(STREET2, UNIT)         AS RESIDENCE_ADDRESS_LINE_2,
       CITY                            AS RESIDENCE_ADDRESS_CITY,
       COALESCE(STATE, 'WV')           AS RESIDENCE_ADDRESS_STATE,
       ZIP                             AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                           AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR(DATE_OF_BIRTH::date)       AS BIRTH_YEAR,
       MONTH(DATE_OF_BIRTH::date)      AS BIRTH_MONTH,
       DAYOFMONTH(DATE_OF_BIRTH::date) AS BIRTH_DATE,
       CASE
           WHEN STATUS = 'Active' THEN 'A'
           WHEN STATUS = 'Inactive' THEN 'I'
           ELSE STATUS END             as VOTER_STATUS,
       REGISTRATION_DATE::date         AS REGISTRATION_DATE,
       NULL                            AS LAST_VOTED_DATE,
       PARTYAFFILIATION                AS LAST_PARTY_VOTED,
       CONGRESSIONAL_DISTRICT          AS CONGRESSIONAL_DISTRICT,
       NULL                            AS STATE_SENATE_DISTRICT,
       NULL                            AS STATE_HOUSE_DISTRICT,
       NULL                            AS JUDICIAL_DISTRICT,
       NULL                            AS COUNTY_COMMISSION_DISTRICT,
       NULL                            AS SCHOOL_BOARD_DISTRICT,
       NULL                            AS CITY_COUNCIL_DISTRICT,
       PRECINCT_NUMBER                 AS COUNTY_PRECINCT,
       NULL                            AS MUNICIPAL_PRECINCT,
       NULL                            AS RACE,
       NULL                            AS GENDER,
       PUBLIC.MERGE_ADDRESS_ELEMENTS([
           MAIL_HOUSE_NO,
           MAIL_STREET
           ])                          AS MAILING_ADDRESS_LINE_1,
       COALESCE(
               MAIL_STREET2,
               MAIL_UNIT
       )                               AS MAILING_ADDRESS_LINE_2,
       NULL                            AS MAILING_LINE_3,
       MAIL_CITY                       AS MAILING_CITY,
       MAIL_STATE                      AS MAILING_STATE,
       MAIL_ZIP                        AS MAILING_ZIPCODE,
       NULL                            AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.WEST_VIRGINIA_VOTERS_2023_11_06 V