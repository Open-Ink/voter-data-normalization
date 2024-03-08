SELECT '2023-12-11'::date                           as FILE_DATE, 'MS' as STATE_CODE,
       CASE
           WHEN RES_COUNTY = 'Jeff Davis'
               THEN 'Jefferson Davis'
           ELSE RES_COUNTY END                      as COUNTY_CODE,
       V.MAPPING_VALUE                              AS VOTER_ID,
       NULL                                         AS PREFIX,
       V.FIRST_NAME                                 AS FIRST_NAME,
       V.MIDDLE_NAME                                AS MIDDLE_NAME,
       V.LAST_NAME                                  AS LAST_NAME,
       NULL                                         AS NAME_SUFFIX,
       V.RESIDENTIAL_ADDRESS                        AS RESIDENCE_ADDRESS_LINE_1,
       NULL                                            RESIDENCE_ADDRESS_LINE_2,
       V.RES_CITY                                   AS RESIDENCE_ADDRESS_CITY,
       COALESCE(V.RES_STATE, 'MS')                  AS RESIDENCE_ADDRESS_STATE,
       V.RES_ZIP_CODE                               AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                        AS RESIDENCE_ADDRESS_COUNTRY,
       NULL                                         AS BIRTH_YEAR,
       NULL                                         AS BIRTH_MONTH,
       NULL                                         AS BIRTH_DATE,
       CASE
           WHEN V.STATUS = 'Active' THEN 'A'
           WHEN V.STATUS = 'Inactive' THEN 'I'
           END                                      as VOTER_STATUS,
       TO_DATE(V.EFFECTIVE_REGN_DATE, 'MM/DD/YYYY') AS REGISTRATION_DATE,
       TO_DATE(V.DATE_VOTED, 'MM/DD/YYYY')          AS LAST_VOTED_DATE,
       NULL                                         AS LAST_PARTY_VOTED,
       V.CONG                                       AS CONGRESSIONAL_DISTRICT,
       V.SEN                                        AS STATE_SENATE_DISTRICT,
       V.REP                                        AS STATE_HOUSE_DISTRICT,
       V.JUD                                        AS JUDICIAL_DISTRICT,
       NULL                                         AS COUNTY_COMMISSION_DISTRICT,
       NULL                                         AS SCHOOL_BOARD_DISTRICT,
       NULL                                         AS CITY_COUNCIL_DISTRICT,
       V.PRECINCT_NAME                              AS COUNTY_PRECINCT,
       NULL                                         AS MUNICIPAL_PRECINCT,
       NULL                                         AS RACE,
       NULL                                         AS GENDER,
       V.MAILING_ADDRESS                            AS MAILING_ADDRESS_LINE_1,
       NULL                                         AS MAILING_ADDRESS_LINE_2,
       NULL                                         AS MAILING_LINE_3,
       V.MAIL_CITY                                  AS MAILING_CITY,
       V.MAIL_STATE                                 AS MAILING_STATE,
       V.MAIL_ZIP_CODE                              AS MAILING_ZIPCODE,
       NULL                                         AS MAILING_COUNTRY
FROM source('raw', 'MISSISSIPPI_RAW_DATA_2023_12_11') V
WHERE V.RES_COUNTY IS NOT NULL