SELECT '2023-10-17'::date                                       as FILE_DATE,
       'WI'                                                     as STATE_CODE,
       REPLACE(
               REPLACE(VOTERS.COUNTY, ' County', ''),
               'St.',
               'Saint')                                              as COUNTY_CODE,
       VOTERS.VOTER_REG_NUMBER                                  AS VOTER_ID,
       NULL                                                     AS PREFIX,
       VOTERS.FIRST_NAME                                        AS FIRST_NAME,
       VOTERS.MIDDLE_NAME                                       AS MIDDLE_NAME,
       VOTERS.LASTNAME                                          AS LAST_NAME,
       VOTERS.SUFFIX                                            AS NAME_SUFFIX,
       PUBLIC.MERGE_ADDRESS_ELEMENTS([
           HOUSE_NUMBER,
           STREET_NAME
           ])                                                   AS RESIDENCE_ADDRESS_LINE_1,
       PUBLIC.MERGE_ADDRESS_ELEMENTS([UNIT_TYPE,
           UNIT_NUMBER])                                        AS RESIDENCE_ADDRESS_LINE_2,
       TRIM(SUBSTR(ADDRESS2, 0, CHARINDEX('WI', ADDRESS2) - 1)) AS RESIDENCE_ADDRESS_CITY,
       COALESCE(STATE, 'WI')                                    AS RESIDENCE_ADDRESS_STATE,
       ZIPCODE                                                  AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                                    AS RESIDENCE_ADDRESS_COUNTRY,
       NULL                                                     AS BIRTH_YEAR,
       NULL                                                     AS BIRTH_MONTH,
       NULL                                                     AS BIRTH_DATE,
       CASE
           WHEN VOTER_STATUS = 'Active' THEN 'A'
           WHEN VOTER_STATUS = 'Inactive' THEN 'I'
           ELSE NULL
           END                                                  as VOTER_STATUS,
       TO_DATE(REGISTRATION_DATE, 'MM/DD/YYYY')                 AS REGISTRATION_DATE,
       NULL                                                     AS LAST_VOTED_DATE,
       NULL                                                     AS LAST_PARTY_VOTED,
       CONGRESSIONAL                                            AS CONGRESSIONAL_DISTRICT,
       STATE_SENATE                                             AS STATE_SENATE_DISTRICT,
       STATE_ASSEMBLY                                           AS STATE_HOUSE_DISTRICT,
       NULL                                                     AS JUDICIAL_DISTRICT,
       NULL                                                     AS COUNTY_COMMISSION_DISTRICT,
       SCHOOL                                                   AS SCHOOL_BOARD_DISTRICT,
       NULL                                                     AS CITY_COUNCIL_DISTRICT,
       NULL                                                     AS COUNTY_PRECINCT,
       NULL                                                     AS MUNICIPAL_PRECINCT,
       NULL                                                     AS RACE,
       NULL                                                     AS GENDER,
       MAILING_ADDRESS1                                         AS MAILING_ADDRESS_LINE_1,
       MAILING_ADDRESS2                                         AS MAILING_ADDRESS_LINE_2,
       NULL                                                     AS MAILING_LINE_3,
       REGEXP_SUBSTR(
               MAILING_CITY_STATE_ZIP,
               '([A-Za-z \' \.]+) ([A-Z]{2}) ([0-9]{4,5})(-[0-9]{4})?',
               1,
               1,
               'e',
               1
       )                                                        AS MAILING_CITY,
       REGEXP_SUBSTR(
               MAILING_CITY_STATE_ZIP,
               '([A-Za-z \' \.]+) ([A-Z]{2}) ([0-9]{4,5})(-[0-9]{4})?',
               1,
               1,
               'e',
               2
       )                                                        AS MAILING_STATE,
       REGEXP_SUBSTR(
               MAILING_CITY_STATE_ZIP,
               '([A-Za-z \' \.]+) ([A-Z]{2}) ([0-9]{4,5})(-[0-9]{4})?',
               1,
               1,
               'e',
               3
       )                                                        AS MAILING_ZIPCODE,
       REGEXP_SUBSTR(
               MAILING_CITY_STATE_ZIP,
               '^([A-Za-z \' \.]+)$',
               1,
               1,
               'e',
               1
       )                                                        AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.WISCONSIN_VOTERS_2023_10_17 Voters