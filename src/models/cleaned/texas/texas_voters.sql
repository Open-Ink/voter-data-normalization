SELECT '2023-11-28 '::date                       as FILE_DATE,
       'TX'                                      as STATE_CODE,
       C.COUNTY_NAME                             as COUNTY_CODE,
       VUID                                         VOTER_ID,
       NULL                                      AS PREFIX,
       FIRST_NAME                                AS FIRST_NAME,
       MIDDLE_NAME                               AS MIDDLE_NAME,
       LASTNAME                                  AS LAST_NAME,
       SUFFIX                                    AS NAME_SUFFIX,
       MERGE_ADDRESS_ELEMENTS([
           PERM_HOUSE_NUMBER,
           PERM_DESIGNATOR,
           PERM_DIRECTIONAL_PREFIX,
           PERM_STREET_NAME,
           PERM_STREET_TYPE,
           PERM_DIRECTIONAL_SUFFIX
           ])                                    AS RESIDENCE_ADDRESS_LINE_1,
       MERGE_ADDRESS_ELEMENTS([
           PERM_UNIT_NUMBER,
           PERM_UNIT_TYPE
           ])                                    AS RESIDENCE_ADDRESS_LINE_2,
       PERM_CITY                                 AS RESIDENCE_ADDRESS_CITY,
       'TX'                                      AS RESIDENCE_ADDRESS_STATE,
       PERM_ZIP_CODE                             AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                     AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR(TO_DATE(DOB, 'YYYYMMDD'))            AS BIRTH_YEAR,
       MONTH(TO_DATE(DOB, 'YYYYMMDD'))           AS BIRTH_MONTH,
       DAYOFMONTH(TO_DATE(DOB, 'YYYYMMDD'))      AS BIRTH_DATE,
       CASE STATUS_CODE
           WHEN 'V' THEN 'A'
           WHEN 'S' THEN 'I'
           WHEN 'C' THEN 'C'
           END                                   as VOTER_STATUS,
       TO_DATE(DATE_OF_REGISTRATION, 'YYYYMMDD') AS REGISTRATION_DATE,
       NULL                                      AS LAST_VOTED_DATE,
       NULL                                      AS LAST_PARTY_VOTED,
       NULL                                      AS CONGRESSIONAL_DISTRICT,
       NULL                                      AS STATE_SENATE_DISTRICT,
       NULL                                      AS STATE_HOUSE_DISTRICT,
       NULL                                      AS JUDICIAL_DISTRICT,
       NULL                                      AS COUNTY_COMMISSION_DISTRICT,
       NULL                                      AS SCHOOL_BOARD_DISTRICT,
       NULL                                      AS CITY_COUNCIL_DISTRICT,
       NULL                                      AS COUNTY_PRECINCT,
       NULL                                      AS MUNICIPAL_PRECINCT,
       NULL                                      AS RACE,
       NULL                                      AS GENDER,
       MAILING_ADDRESS_1                         AS MAILING_ADDRESS_LINE_1,
       MAILING_ADDRESS_2                         AS MAILING_ADDRESS_LINE_2,
       NULL                                      AS MAILING_LINE_3,
       MAILING_CITY                              AS MAILING_CITY,
       MAILING_STATE                             AS MAILING_STATE,
       MAILING_ZIPCODE                           AS MAILING_ZIPCODE,
       NULL                                      AS MAILING_COUNTRY
FROM {{ source('raw', 'TEXAS_PROCESSED_DATA_2023_11_28') }} V
         INNER JOIN {{ ref("texas_counties") }} C
                    ON V.COUNTY_CODE = C.COUNTY_ID
WHERE VUID NOT IN (
                   '2002582657',
                   '1031674244',
                   '1031674244',
                   '2002582657'
    )
