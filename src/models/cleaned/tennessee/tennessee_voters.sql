SELECT '2024-01-01'::date                       as FILE_DATE,
       'TN'                                     as STATE_CODE,
       C.COUNTY_NAME                            as COUNTY_CODE,
       VOTER_ID                                 AS VOTER_ID,
       NULL                                     AS PREFIX,
       FIRST_NAME                               AS FIRST_NAME,
       MIDDLE_NAME                              AS MIDDLE_NAME,
       LAST_NAME                                AS LAST_NAME,
       NAME_SUFFIX                              AS NAME_SUFFIX,
       RES_ADDRESS1                             AS RESIDENCE_ADDRESS_LINE_1,
       RES_ADDRESS2                             AS RESIDENCE_ADDRESS_LINE_2,
       RES_CITY                                 AS RESIDENCE_ADDRESS_CITY,
       COALESCE(RES_STATE, 'TN')                AS RESIDENCE_ADDRESS_STATE,
       RES_ZIP1                                 AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                    AS RESIDENCE_ADDRESS_COUNTRY,
       BIRTH_YEAR::number                       AS BIRTH_YEAR,
       BIRTH_MONTH::number                      AS BIRTH_MONTH,
       BIRTH_DAY::number                       AS BIRTH_DATE,
       STATUS                                   as VOTER_STATUS,
       TO_DATE(REGISTRATION_DATE, 'YYYY-MM-DD') AS REGISTRATION_DATE,
       NULL                                     AS LAST_VOTED_DATE,
       COALESCE(AUG_22_PARTY,
                AUG_20_PARTY,
                AUG_18_PARTY,
                AUG_16_PARTY,
                AUG_14_PARTY,
                AUG_12_PARTY,
                AUG_10_PARTY,
                AUG_08_PARTY,
                AUG_06_PARTY,
                AUG_04_PARTY,
                AUG_02_PARTY,
                AUG_00_PARTY)                   AS LAST_PARTY_VOTED,
       US_CONGRESS                              AS CONGRESSIONAL_DISTRICT,
       TN_SENATE                                AS STATE_SENATE_DISTRICT,
       TN_HOUSE                                 AS STATE_HOUSE_DISTRICT,
       JUDICIAL                                 AS JUDICIAL_DISTRICT,
       NULL                                     AS COUNTY_COMMISSION_DISTRICT,
       NULL                                     AS SCHOOL_BOARD_DISTRICT,
       NULL                                     AS CITY_COUNCIL_DISTRICT,
       PRECINCT                                 AS COUNTY_PRECINCT,
       NULL                                     AS MUNICIPAL_PRECINCT,
       CASE
           WHEN RACE = 'W' THEN 'White'
           WHEN RACE = 'B' THEN 'Black'
           WHEN RACE = 'O' THEN 'Other'
           ELSE 'Unknown'
           END                                  AS RACE,
       GENDER                                   AS GENDER,
       NULL                                     AS MAILING_ADDRESS_LINE_1,
       NULL                                     AS MAILING_ADDRESS_LINE_2,
       NULL                                     AS MAILING_LINE_3,
       NULL                                     AS MAILING_CITY,
       NULL                                     AS MAILING_STATE,
       NULL                                     AS MAILING_ZIPCODE,
       NULL                                     AS MAILING_COUNTRY
FROM {{ source('raw', 'TENNESSEE_VOTERS_2024_01_01') }} V
         INNER JOIN {{ ref("tennessee_counties") }} C ON V.COUNTY = C.COUNTY_ID