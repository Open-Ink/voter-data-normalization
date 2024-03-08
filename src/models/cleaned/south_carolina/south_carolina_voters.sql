SELECT '2024-01-04'::date                             as FILE_DATE,
       'SC'                                           as STATE_CODE,
       COUNTY.county_name                             as COUNTY_CODE,
       VOTERS.VOTER_REGISTRATION_NUMBER               AS VOTER_ID,
       NULL                                           AS PREFIX,
       VOTERS.FIRST_NAME                              AS FIRST_NAME,
       VOTERS.MIDDLE_NAME                             AS MIDDLE_NAME,
       VOTERS.LAST_NAME                               AS LAST_NAME,
       VOTERS.SUFFIX                                  AS NAME_SUFFIX,
       MERGE_ADDRESS_ELEMENTS(
               [RESIDENCE_LINE_NUMBER,
                   RESIDENCE_LINE_NAME])              AS RESIDENCE_ADDRESS_LINE_1,
       RESIDENCE_LINE_SUITE                           AS RESIDENCE_ADDRESS_LINE_2,
       RESIDENCE_CITY                                 AS RESIDENCE_ADDRESS_CITY,
       'SC'                                           AS RESIDENCE_ADDRESS_STATE,
       RESIDENCE_ZIPCODE                              AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                          AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR(TO_DATE(DATE_OF_BIRTH, 'YYYYMMDD'))       AS BIRTH_YEAR,
       MONTH(TO_DATE(DATE_OF_BIRTH, 'YYYYMMDD'))      AS BIRTH_MONTH,
       DAYOFMONTH(TO_DATE(DATE_OF_BIRTH, 'YYYYMMDD')) AS BIRTH_DATE,
       CASE
           WHEN VOTER_STATUS = 'Active' THEN 'A'
           WHEN VOTER_STATUS = 'Inactive' THEN 'I'
           ELSE VOTER_STATUS
           END                                        as VOTER_STATUS,
       TO_DATE(REGISTRATION_DATE, 'YYYYMMDD')         AS REGISTRATION_DATE,
       TO_DATE(DATE_LAST_VOTED, 'YYYYMMDD')           AS LAST_VOTED_DATE,
       NULL                                           AS LAST_PARTY_VOTED,
       CONGRESSIONAL_DISTRICT                         AS CONGRESSIONAL_DISTRICT,
       SENATE_DISTRICT                                AS STATE_SENATE_DISTRICT,
       HOUSE_DISTRICT                                 AS STATE_HOUSE_DISTRICT,
       NULL                                           AS JUDICIAL_DISTRICT,
       NULL                                           AS COUNTY_COMMISSION_DISTRICT,
       SCHOOL_DISTRICT                                AS SCHOOL_BOARD_DISTRICT,
       CITY_COUNCIL_DISTRICT                          AS CITY_COUNCIL_DISTRICT,
       PRECINCT_NAME                                  AS COUNTY_PRECINCT,
       NULL                                           AS MUNICIPAL_PRECINCT,
       CASE
           WHEN RACE = 'Unknown' THEN 'Unknown'
           WHEN RACE = 'Black/African' THEN 'Black'
           WHEN RACE = 'Asian' THEN 'Asian'
           WHEN RACE = 'Hispanic' THEN 'Hispanic/Latino'
           WHEN RACE = 'Other' THEN 'Other'
           WHEN RACE = 'White' THEN 'White'
           WHEN RACE = 'Native American' THEN 'American Indian or Alaskan Native'
           WHEN RACE = 'M' THEN 'Multi-racial'
           ELSE RACE
           END                                        AS RACE,
       CASE
           WHEN GENDER = 'Male' THEN 'M'
           WHEN GENDER = 'Female' THEN 'F'
           ELSE 'U' END                               AS GENDER,
       MERGE_ADDRESS_ELEMENTS(
               [MAILING_LINE_NUMBER,
                   MAILING_LINE_NAME])                AS MAILING_ADDRESS_LINE_1,
       NULL                                           AS MAILING_ADDRESS_LINE_2,
       NULL                                           AS MAILING_LINE_3,
       MAILING_CITY                                   AS MAILING_CITY,
       MAILING_STATE                                  AS MAILING_STATE,
       MAILING_ZIPCODE                                AS MAILING_ZIPCODE,
       NULL                                           AS MAILING_COUNTRY
FROM {{ source('raw', 'SOUTH_CAROLINA_VOTERS_2024_01_04') }} Voters
         INNER JOIN {{ ref("south_carolina_counties") }} COUNTY ON VOTERS.COUNTY = COUNTY.county_id