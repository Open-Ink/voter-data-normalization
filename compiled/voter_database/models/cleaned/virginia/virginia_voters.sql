SELECT '2023-11-17'::date                       as FILE_DATE,
       'VA'                                     as STATE_CODE,
       REPLACE(
               REPLACE(
                       LOCALITYNAME,
                       ' COUNTY',
                       ''),
               '&',
               'AND')                           as COUNTY_CODE,
       IDENTIFICATION_NUMBER                       VOTER_ID,
       NULL                                     AS PREFIX,
       FIRST_NAME                               AS FIRST_NAME,
       MIDDLE_NAME                              AS MIDDLE_NAME,
       LAST_NAME                                AS LAST_NAME,
       SUFFIX                                   AS NAME_SUFFIX,
       MERGE_ADDRESS_ELEMENTS([
           HOUSE_NUMBER,
           HOUSENUMBERSUFFIX,
           STREET_NAME,
           STREETTYPECODENAME,
           DIRECTION,
           POST_DIRECTION
           ])                                   AS RESIDENCE_ADDRESS_LINE_1,
       MERGE_ADDRESS_ELEMENTS([
           APT_NUM
           ])                                   AS RESIDENCE_ADDRESS_LINE_2,
       CITY                                     AS RESIDENCE_ADDRESS_CITY,
       COALESCE(STATE, 'VA')                    AS RESIDENCE_ADDRESS_STATE,
       ZIP                                      AS RESIDENCE_ADDRESS_ZIPCODE,
       'USA'                                    AS RESIDENCE_ADDRESS_COUNTRY,
       YEAR(TO_DATE(DOB, 'MM/DD/YYYY'))         AS BIRTH_YEAR,
       MONTH(TO_DATE(DOB, 'MM/DD/YYYY'))        AS BIRTH_MONTH,
       DAYOFMONTH(TO_DATE(DOB, 'MM/DD/YYYY'))   AS BIRTH_DATE,
       CASE STATUS
           WHEN 'Active' THEN 'A'
           WHEN 'Inactive' THEN 'I'
           END                                  as VOTER_STATUS,
       TO_DATE(REGISTRATION_DATE, 'MM/DD/YYYY') AS REGISTRATION_DATE,
       NULL                                     AS LAST_VOTED_DATE,
       NULL                                     AS LAST_PARTY_VOTED,
       CONG_CODE_VALUE                          AS CONGRESSIONAL_DISTRICT,
       STSENATE_CODE_VALUE                      AS STATE_SENATE_DISTRICT,
       STHOUSE_CODE_VALUE                       AS STATE_HOUSE_DISTRICT,
       NULL                                     AS JUDICIAL_DISTRICT,
       NULL                                     AS COUNTY_COMMISSION_DISTRICT,
       NULL                                     AS SCHOOL_BOARD_DISTRICT,
       NULL                                     AS CITY_COUNCIL_DISTRICT,
       NULL                                     AS COUNTY_PRECINCT,
       NULL                                     AS MUNICIPAL_PRECINCT,
       NULL                                     AS RACE,
       GENDER                                   AS GENDER,
       MAILING_ADDRESS_LINE_1                   AS MAILING_ADDRESS_LINE_1,
       MAILING_ADDRESS_LINE_2                   AS MAILING_ADDRESS_LINE_2,
       MAILING_ADDRESS_LINE_3                   AS MAILING_LINE_3,
       MAILING_CITY                             AS MAILING_CITY,
       MAILING_STATE                            AS MAILING_STATE,
       MAILING_ZIP                              AS MAILING_ZIPCODE,
       NULL                                     AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.VIRGINIA_VOTERS_2023_11_17