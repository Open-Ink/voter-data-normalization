SELECT DISTINCT '2023-11-30'::date                       as FILE_DATE,
                'CO'                                     as STATE_CODE,
                COUNTY                                   as COUNTY_CODE,
                VOTER_ID                                 AS VOTER_ID,
                NULL                                     AS PREFIX,
                FIRST_NAME                               AS FIRST_NAME,
                MIDDLE_NAME                              AS MIDDLE_NAME,
                LAST_NAME                                AS LAST_NAME,
                NAME_SUFFIX                              AS NAME_SUFFIX,
                {{ target.schema }}.MERGE_ADDRESS_ELEMENTS([
                    HOUSE_NUM,
                    HOUSE_SUFFIX,
                    PRE_DIR,
                    STREET_NAME,
                    STREET_TYPE,
                    POST_DIR
                    ])                                   AS RESIDENCE_ADDRESS_LINE_1,
                {{ target.schema }}.MERGE_ADDRESS_ELEMENTS([
                    UNIT_TYPE,
                    UNIT_NUM
                    ])                                   AS RESIDENCE_ADDRESS_LINE_2,
                RESIDENTIAL_CITY                         AS RESIDENCE_ADDRESS_CITY,
                RESIDENTIAL_STATE                        AS RESIDENCE_ADDRESS_STATE,
                RESIDENTIAL_ZIP_CODE                     AS RESIDENCE_ADDRESS_ZIPCODE,
                'USA'                                    AS RESIDENCE_ADDRESS_COUNTRY,
                COLORADO_VOTERS_2023_11_30.BIRTH_YEAR    AS BIRTH_YEAR,
                NULL                                     AS BIRTH_MONTH,
                NULL                                     AS BIRTH_DATE,
                CASE
                    WHEN STATUS = 'Active' THEN 'A'
                    WHEN STATUS = 'Inactive' THEN 'I'
                    ELSE STATUS
                    END                                  as VOTER_STATUS,
                TO_DATE(REGISTRATION_DATE, 'MM/DD/YYYY') AS REGISTRATION_DATE,
                NULL                                     AS LAST_VOTED_DATE,
                PARTY                                    AS LAST_PARTY_VOTED,
                CONGRESSIONAL                            AS CONGRESSIONAL_DISTRICT,
                STATE_SENATE                             AS STATE_SENATE_DISTRICT,
                STATE_HOUSE                              AS STATE_HOUSE_DISTRICT,
                NULL                                     AS JUDICIAL_DISTRICT,
                NULL                                     AS COUNTY_COMMISSION_DISTRICT,
                NULL                                     AS SCHOOL_BOARD_DISTRICT,
                NULL                                     AS CITY_COUNCIL_DISTRICT,
                PRECINCT_NAME                            AS COUNTY_PRECINCT,
                NULL                                     AS MUNICIPAL_PRECINCT,
                NULL                                     AS RACE,
                NULL                                     AS GENDER,
                MAIL_ADDR1                               AS MAILING_ADDRESS_LINE_1,
                MAIL_ADDR2                               AS MAILING_ADDRESS_LINE_2,
                MAIL_ADDR3                               AS MAILING_LINE_3,
                MAILING_CITY                             AS MAILING_CITY,
                MAILING_STATE                            AS MAILING_STATE,
                MAILING_ZIP_CODE                         AS MAILING_ZIPCODE,
                MAILING_COUNTRY                          AS MAILING_COUNTRY
FROM {{ source('raw', 'COLORADO_VOTERS_2023_11_30') }}
