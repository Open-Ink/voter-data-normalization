SELECT '2023-09-25'::date                as FILE_DATE,
       'NC'                              as STATE_CODE,
       VOTERS.COUNTY_DESC                as COUNTY_CODE,
       VOTERS.VOTER_REG_NUM              AS VOTER_ID,
       NULL                              AS PREFIX,
       VOTERS.FIRST_NAME                 AS FIRST_NAME,
       VOTERS.MIDDLE_NAME                AS MIDDLE_NAME,
       VOTERS.LAST_NAME                  AS LAST_NAME,
       VOTERS.NAME_SUFFIX_LBL            AS NAME_SUFFIX,
       RES_STREET_ADDRESS                AS RESIDENCE_ADDRESS_LINE_1,
       NULL                              AS RESIDENCE_ADDRESS_LINE_2,
       RES_CITY_DESC                     AS RESIDENCE_ADDRESS_CITY,
       COALESCE(STATE_CD, 'NC')          AS RESIDENCE_ADDRESS_STATE,
       ZIP_CODE                          AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                             AS RESIDENCE_ADDRESS_COUNTRY,
       BIRTH_YEAR                        AS BIRTH_YEAR,
       NULL                              AS BIRTH_MONTH,
       NULL                              AS BIRTH_DATE,
       CASE
           WHEN STATUS_CD = 'A' THEN 'A'
           WHEN STATUS_CD = 'I' THEN 'I'
           WHEN STATUS_CD = 'D' THEN 'D'
           WHEN STATUS_CD = 'R' THEN 'R'
           WHEN STATUS_CD = 'T' THEN 'S'
           ELSE STATUS_CD
           END                           as VOTER_STATUS,
       TO_DATE(REGISTR_DT, 'MM/DD/YYYY') AS REGISTRATION_DATE,
       NULL                              AS LAST_VOTED_DATE,
       PARTY_CD                          AS LAST_PARTY_VOTED,
       CONG_DIST_ABBRV                   AS CONGRESSIONAL_DISTRICT,
       NC_SENATE_ABBRV                   AS STATE_SENATE_DISTRICT,
       NC_HOUSE_ABBRV                    AS STATE_HOUSE_DISTRICT,
       JUDIC_DIST_ABBRV                  AS JUDICIAL_DISTRICT,
       COUNTY_COMMISS_DESC               AS COUNTY_COMMISSION_DISTRICT,
       SCHOOL_DIST_DESC                  AS SCHOOL_BOARD_DISTRICT,
       NULL                              AS CITY_COUNCIL_DISTRICT,
       PRECINCT_DESC                     AS COUNTY_PRECINCT,
       NULL                              AS MUNICIPAL_PRECINCT,
       CASE
           WHEN RACE_CODE = 'A' THEN 'Asian'
           WHEN RACE_CODE = 'B' THEN 'Black'
           WHEN RACE_CODE = 'I' THEN 'American Indian or Alaskan Native'
           WHEN RACE_CODE = 'M' THEN 'Multi-racial'
           WHEN RACE_CODE = 'O' THEN 'Other'
           WHEN RACE_CODE = 'P' THEN 'Pacific Islander'
           WHEN RACE_CODE = 'U' THEN 'Unknown'
           WHEN RACE_CODE = 'W' THEN 'White'
           END                           AS RACE,
       CASE
           WHEN GENDER_CODE = 'M' THEN 'M'
           WHEN GENDER_CODE = 'F' THEN 'F'
           ELSE 'U' END                  AS GENDER,
       MAIL_ADDR1                        AS MAILING_ADDRESS_LINE_1,
       MAIL_ADDR2                        AS MAILING_ADDRESS_LINE_2,
       MAIL_ADDR3                        AS MAILING_LINE_3,
       MAIL_CITY                         AS MAILING_CITY,
       MAIL_STATE                        AS MAILING_STATE,
       MAIL_ZIPCODE                      AS MAILING_ZIPCODE,
       MAIL_ADDR4                        AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.NORTH_CAROLINA_VOTERS_2023_07_21 Voters