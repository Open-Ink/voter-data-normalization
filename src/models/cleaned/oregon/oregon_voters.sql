SELECT '2023-11-01'::date                   as FILE_DATE,
       'OR'                                 as STATE_CODE,
       COUNTY                               as COUNTY_CODE,
       VOTER_ID                             AS VOTER_ID,
       NULL                                 AS PREFIX,
       VOTERS.FIRST_NAME                    AS FIRST_NAME,
       VOTERS.MIDDLE_NAME                   AS MIDDLE_NAME,
       VOTERS.LAST_NAME                     AS LAST_NAME,
       VOTERS.NAME_SUFFIX                   AS NAME_SUFFIX,
       RES_ADDRESS_1                        AS RESIDENCE_ADDRESS_LINE_1,
       RES_ADDRESS_2                        AS RESIDENCE_ADDRESS_LINE_2,
       CITY                                 AS RESIDENCE_ADDRESS_CITY,
       COALESCE(STATE, 'OR')                AS RESIDENCE_ADDRESS_STATE,
       ZIP_CODE                             AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                AS RESIDENCE_ADDRESS_COUNTRY,
       BIRTH_DATE                           AS BIRTH_YEAR,
       NULL                                 AS BIRTH_MONTH,
       NULL                                 AS BIRTH_DATE,
       STATUS                               as VOTER_STATUS,
       TO_DATE(EFF_REGN_DATE, 'MM-DD-YYYY') AS REGISTRATION_DATE,
       NULL                                 AS LAST_VOTED_DATE,
       PARTY_CODE                           AS LAST_PARTY_VOTED,
       NULL                                 AS CONGRESSIONAL_DISTRICT,
       NULL                                 AS STATE_SENATE_DISTRICT,
       NULL                                 AS STATE_HOUSE_DISTRICT,
       NULL                                 AS JUDICIAL_DISTRICT,
       NULL                                 AS COUNTY_COMMISSION_DISTRICT,
       NULL                                 AS SCHOOL_BOARD_DISTRICT,
       NULL                                 AS CITY_COUNCIL_DISTRICT,
       PRECINCT_NAME                        AS COUNTY_PRECINCT,
       NULL                                 AS MUNICIPAL_PRECINCT,
       NULL                                 AS RACE,
       NULL                                 AS GENDER,
       EFF_ADDRESS_1                        AS MAILING_ADDRESS_LINE_1,
       NULL                                 AS MAILING_ADDRESS_LINE_2,
       EFF_ADDRESS_3                        AS MAILING_LINE_3,
       EFF_CITY                             AS MAILING_CITY,
       EFF_STATE                            AS MAILING_STATE,
       EFF_ZIP_CODE                         AS MAILING_ZIPCODE,
       EFF_ADDRESS_4                        AS MAILING_COUNTRY
FROM {{ source('raw', 'OREGON_VOTERS_2023_11_01') }} VOTERS
WHERE UNLISTED != 'ACP'
  AND TRY_TO_NUMBER(BIRTH_DATE) IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY VOTER_ID ORDER BY TRY_TO_DATE(EFF_REGN_DATE, 'MM-DD-YYYY')) = 1
