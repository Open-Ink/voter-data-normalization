SELECT '2023-09-20 '::date                  as FILE_DATE,
       'NY'                                 as STATE_CODE,
       C.COUNTY_NAME                        as COUNTY_CODE,
       SBOE_ID                              AS VOTER_ID,
       UPPER(REPLACE(MERGE_ADDRESS_ELEMENTS([
           R_ADD_NUMBER,
           R_HALF_CODE,
           R_PRE_DIRECTION,
           R_STREET_NAME,
           R_POST_DIRECTION,
           R_APARTMENT_TYPE,
           R_APARTMENT,
           R_ZIP5
           ]), ' ', ''))                    AS UNIQUE_ID,
       NULL                                 AS PREFIX,
       FIRST_NAME                           AS FIRST_NAME,
       MIDDLE_NAME                          AS MIDDLE_NAME,
       LAST_NAME                            AS LAST_NAME,
       NAME_SUFFIX                          AS NAME_SUFFIX,
       CASE
           WHEN R_ADD_NUMBER IS NULL THEN R_ADDR_NON_STD
           ELSE MERGE_ADDRESS_ELEMENTS([
               R_ADD_NUMBER,
               R_HALF_CODE,
               R_PRE_DIRECTION,
               R_STREET_NAME,
               R_POST_DIRECTION
               ]) END                       AS RESIDENCE_ADDRESS_LINE_1,
       MERGE_ADDRESS_ELEMENTS([
           R_APARTMENT_TYPE,
           R_APARTMENT
           ])                               AS RESIDENCE_ADDRESS_LINE_2,
       R_CITY                               AS RESIDENCE_ADDRESS_CITY,
       'NY'                                 AS RESIDENCE_ADDRESS_STATE,
       R_ZIP5                               AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR(TO_DATE(DOB, 'YYYYMMDD'))       AS BIRTH_YEAR,
       MONTH(TO_DATE(DOB, 'YYYYMMDD'))      AS BIRTH_MONTH,
       DAYOFMONTH(TO_DATE(DOB, 'YYYYMMDD')) AS BIRTH_DATE,
       CASE
           WHEN STATUS IN ('A', 'AM', 'AF', 'AP', 'AU') THEN 'A'
           WHEN STATUS = 'I' THEN 'I'
           WHEN STATUS = 'P' THEN 'C'
           WHEN STATUS = '17' THEN 'PREREG'
           ELSE STATUS
           END                              as VOTER_STATUS,
       TO_DATE(REG_DATE, 'YYYYMMDD')        AS REGISTRATION_DATE,
       TO_DATE(LAST_VOTER_DATE, 'YYYYMMDD') AS LAST_VOTED_DATE,
       ENROLLMENT                           AS LAST_PARTY_VOTED,
       CD_NUMBER                            AS CONGRESSIONAL_DISTRICT,
       SD_NUMBER                            AS STATE_SENATE_DISTRICT,
       AD_NUMBER                            AS STATE_HOUSE_DISTRICT,
       NULL                                 AS JUDICIAL_DISTRICT,
       NULL                                 AS COUNTY_COMMISSION_DISTRICT,
       NULL                                 AS SCHOOL_BOARD_DISTRICT,
       NULL                                 AS CITY_COUNCIL_DISTRICT,
       ED_NUMBER                            AS COUNTY_PRECINCT,
       NULL                                 AS MUNICIPAL_PRECINCT,
       NULL                                 AS RACE,
       NULL                                 AS GENDER,
       MAIL_ADD_1                           AS MAILING_ADDRESS_LINE_1,
       MAIL_ADD_2                           AS MAILING_ADDRESS_LINE_2,
       MAIL_ADD_3                           AS MAILING_LINE_3,
       NULL                                 AS MAILING_CITY,
       NULL                                 AS MAILING_STATE,
       NULL                                 AS MAILING_ZIPCODE,
       MAIL_ADD_4                           AS MAILING_COUNTRY
FROM {{ source('raw', 'NEW_YORK_VOTERS_2023_09_20') }} V
         INNER JOIN {{ ref("new_york_counties") }} C ON V.COUNTY_CODE_NUMBER = C.COUNTY_ID
WHERE STATUS != 'P'
  AND SBOE_ID NOT IN (
                  'NY000000000022704274',
                  'NY000000000053764904',
                  'NY000000000037135509',
                  'NY000000000038037707',
                  'NY000000000037224350',
                  'NY000000000037594247',
                  'NY000000000037465236',
                  'NY000000000054520976',
                  'NY000000000037892323',
                  'NY000000000022592629',
                  'NY000000000052822077',
                  'NY000000000054289529',
                  'NY000000000037780716',
                  'NY000000000037893454',
                  'NY000000000037771799',
                  'NY000000000035652286',
                  'NY000000000037929514',
                  'NY000000000050381008',
                  'NY000000000037910728',
                  'NY000000000034884031',
                  'NY000000000036286704',
                  'NY000000000034729033',
                  'NY000000000037529297',
                  'NY000000000037911976',
                  'NY000000000037596418',
                  'NY000000000038240968',
                  'NY000000000057661775',
                  'NY000000000037403834',
                  'NY000000000040014297',
                  'NY000000000053248441'
    )
QUALIFY ROW_NUMBER() OVER (PARTITION BY SBOE_ID ORDER BY STATUS) = 1
{# I've removed duplicates based on status. Active comes before inactive and purged.
This will make sure that if there are duplicates, only the active one is taken in.
#}