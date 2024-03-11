SELECT '2023-11-24 '::date                    as FILE_DATE,
       'NJ'                                   as STATE_CODE,
       VOTERS.COUNTY                          as COUNTY_CODE,
       VOTERS.DISPLAY_ID                      AS VOTER_ID,
       NULL                                   AS PREFIX,
       VOTERS.FIRST_NAME                      AS FIRST_NAME,
       VOTERS.MIDDLE_NAME                     AS MIDDLE_NAME,
       VOTERS.LAST_NAME                       AS LAST_NAME,
       VOTERS.NAME_SUFFIX                     AS NAME_SUFFIX,
       PUBLIC.MERGE_ADDRESS_ELEMENTS([
           STREET_NUM,
           STREET_PRE,
           STREET_POST,
           STREET_BASE,
           STREET_SUFFIX,
           STREET_NAME
           ])                                 AS RESIDENCE_ADDRESS_LINE_1,
       APT_UNIT                               AS RESIDENCE_ADDRESS_LINE_2,
       CITY                                   AS RESIDENCE_ADDRESS_CITY,
       'NJ'                                   AS RESIDENCE_ADDRESS_STATE,
       ZIP                                    AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                  AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR(TO_DATE(DOB, 'YYYY-MM-DD'))       AS BIRTH_YEAR,
       MONTH(TO_DATE(DOB, 'YYYY-MM-DD'))      AS BIRTH_MONTH,
       DAYOFMONTH(TO_DATE(DOB, 'YYYY-MM-DD')) AS BIRTH_DATE,
       CASE STATUS
           WHEN 'Active' THEN 'A'
           WHEN 'Pending' THEN 'P'
           WHEN 'Pending Need ID' THEN 'P'
           WHEN 'Pending-17' THEN 'P'
           WHEN 'Pending-17 Need ID' THEN 'P'
           WHEN 'Inactive Confirmation' THEN 'I'
           WHEN 'Inactive Confirmation Need ID' THEN 'I'
           WHEN 'Active State & Federal Elections' THEN 'A'
           ELSE 'A'
           END                                as VOTER_STATUS,
       NULL                                   AS REGISTRATION_DATE,
       NULL                                   AS LAST_VOTED_DATE,
       PARTY                                  AS LAST_PARTY_VOTED,
       CONGRESSIONAL                          AS CONGRESSIONAL_DISTRICT,
       NULL                                   AS STATE_SENATE_DISTRICT,
       LEGISLATIVE                            AS STATE_HOUSE_DISTRICT,
       NULL                                   AS JUDICIAL_DISTRICT,
       NULL                                   AS COUNTY_COMMISSION_DISTRICT,
       SCHOOL                                 AS SCHOOL_BOARD_DISTRICT,
       NULL                                   AS CITY_COUNCIL_DISTRICT,
       NULL                                   AS COUNTY_PRECINCT,
       NULL                                   AS MUNICIPAL_PRECINCT,
       NULL                                   AS RACE,
       NULL                                   AS GENDER,
       NULL                                   AS MAILING_ADDRESS_LINE_1,
       NULL                                   AS MAILING_ADDRESS_LINE_2,
       NULL                                   AS MAILING_LINE_3,
       NULL                                   AS MAILING_CITY,
       NULL                                   AS MAILING_STATE,
       NULL                                   AS MAILING_ZIPCODE,
       NULL                                   AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.NEW_JERSEY_VOTERS_2023_11_24 Voters