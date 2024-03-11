SELECT DISTINCT '2023-11-21'::date                              as FILE_DATE,
                'MI'                                            as STATE_CODE,
                COUNTY_NAME                                     as COUNTY_CODE,
                VOTERS.VOTER_IDENTIFICATION_NUMBER              AS VOTER_ID,
                NULL                                            AS PREFIX,
                VOTERS.FIRST_NAME,
                VOTERS.MIDDLE_NAME,
                VOTERS.LAST_NAME,
                VOTERS.NAME_SUFFIX                              AS NAME_SUFFIX,
                PUBLIC.MERGE_ADDRESS_ELEMENTS([STREET_NUMBER_PREFIX,
                    STREET_NUMBER, STREET_NUMBER_SUFFIX, DIRECTION_PREFIX,
                    STREET_NAME, STREET_TYPE, DIRECTION_SUFFIX
                    ])                                          AS RESIDENCE_ADDRESS_LINE_1,
                VOTERS.EXTENSION                                AS RESIDENCE_ADDRESS_LINE_2,
                VOTERS.CITY                                     AS RESIDENCE_ADDRESS_CITY,
                'MI'                                            AS RESIDENCE_ADDRESS_STATE,
                VOTERS.ZIP_CODE                                 AS RESIDENCE_ADDRESS_ZIPCODE,
                'USA'                                           AS RESIDENCE_ADDRESS_COUNTRY,
                VOTERS.YEAR_OF_BIRTH                            AS BIRTH_YEAR,
                NULL                                            AS BIRTH_MONTH,
                NULL                                            AS BIRTH_DATE,
                CASE
                    WHEN VOTER_STATUS_TYPE_CODE = 'A' THEN 'A'
                    WHEN VOTER_STATUS_TYPE_CODE = 'V' THEN 'I'
                    ELSE VOTER_STATUS_TYPE_CODE
                    END                                         as VOTER_STATUS,
                TO_DATE(VOTERS.REGISTRATION_DATE, 'YYYY-MM-DD') AS REGISTRATION_DATE,
                NULL                                            AS LAST_VOTED_DATE,
                NULL                                            AS LAST_PARTY_VOTED,
                US_CONGRESS_DISTRICT_NAME                       AS CONGRESSIONAL_DISTRICT,
                STATE_SENATE_DISTRICT_NAME                      AS STATE_SENATE_DISTRICT,
                STATE_HOUSE_DISTRICT_NAME                       AS STATE_HOUSE_DISTRICT,
                NULL                                            AS JUDICIAL_DISTRICT,
                VOTERS.COUNTY_COMMISSIONER_DISTRICT_NAME        AS COUNTY_COMMISSION_DISTRICT,
                VOTERS.SCHOOL_DISTRICT_NAME                     AS SCHOOL_BOARD_DISTRICT,
                NULL                                            AS CITY_COUNCIL_DISTRICT,
                PRECINCT                                        AS COUNTY_PRECINCT,
                NULL                                            AS MUNICIPAL_PRECINCT,
                NULL                                            AS RACE,
                CASE
                    WHEN VOTERS.GENDER = 'M' THEN 'M'
                    WHEN VOTERS.GENDER = 'F' THEN 'F'
                    WHEN VOTERS.GENDER = 'U' THEN 'U'
                    ELSE VOTERS.GENDER END                      AS GENDER,
                MAILING_ADDRESS_LINE_ONE                        AS MAILING_ADDRESS_LINE_1,
                MAILING_ADDRESS_LINE_TWO                        AS MAILING_ADDRESS_LINE_2,
                NULL                                            AS MAILING_LINE_3,
                MAILING_ADDRESS_LINE_THREE                      AS MAILING_CITY,
                VOTERS.MAILING_ADDRESS_LINE_FOUR                AS MAILING_STATE,
                NULL                                            AS MAILING_ZIPCODE,
                MAILING_ADDRESS_LINE_FIVE                       AS MAILING_COUNTRY
FROM DBT_VOTER_DATA.raw.MICHIGAN_VOTERS_2023_11_21 VOTERS
WHERE NOT (VOTER_IDENTIFICATION_NUMBER = 160644892 AND RESIDENCE_ADDRESS_LINE_1 = '13635 PINEWOOD ST')
  AND NOT (VOTER_IDENTIFICATION_NUMBER = 34558448 AND RESIDENCE_ADDRESS_LINE_1 = '6021 VANDERBILT DR')